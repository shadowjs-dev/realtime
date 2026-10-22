import { apiRoutes } from "@shadow-js/dusk/features/api-routes";
import { streams } from "@shadow-js/dusk/features/streams";
import { type ServerHandle, serve } from "@shadow-js/dusk/node";
import { createHandler, type DuskRuntimeManifest } from "@shadow-js/dusk/server";
import { afterEach, describe, expect, it } from "vitest";
import { decodeServerFrame, type ServerFrame } from "../src/index";
import { createHub, type Hub } from "../src/server/index";

/**
 * The hub inside a real Dusk server over real sockets: `export const WS = hub.websocket()`, `export const SSE = hub.sse()` and
 * `export const POST = hub.post()` as the guide shows them, reached with the platform's own `WebSocket` and `fetch`.
 */

const servers: ServerHandle[] = [];
const hubs: Hub[] = [];
afterEach(async () => {
  for (const server of servers.splice(0)) await server.close();
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
});

async function boot(hub: Hub, authenticate?: Parameters<Hub["websocket"]>[0]) {
  const route = hub.websocket(authenticate);
  const handler = createHandler({
    features: [streams(), apiRoutes()],
    manifest: {
      routes: [],
      apiRoutes: [
        {
          id: "rt",
          path: "/rt/send",
          load: async () => ({ POST: hub.post(authenticate) as never }),
        },
      ],
      serverFns: {},
      assets: { entry: "/app.js", css: [], preloads: {}, base: "" },
      App: (props: { children?: unknown }) => props.children,
      streams: [
        {
          id: "/rt",
          path: "/rt",
          kinds: ["WS", "SSE"],
          load: async () => ({ WS: route as never, SSE: hub.sse(authenticate) as never }),
        },
      ] satisfies NonNullable<DuskRuntimeManifest["streams"]>,
    },
    dev: true,
    // Node's WebSocket and fetch send no Origin header, as a non-browser client; a browser always sends one.
    security: { csrf: { allowMissingOrigin: true } },
    onError: () => undefined,
    onWarning: () => undefined,
  });
  const handle = await serve(handler, {
    port: 0,
    host: "127.0.0.1",
    compress: false,
    shutdownSignals: false,
    exitOnSignal: false,
    onError: () => undefined,
  });
  servers.push(handle);
  return handle.port;
}

function socket(port: number): {
  readonly frames: ServerFrame[];
  readonly ws: WebSocket;
  ready: Promise<void>;
  until: (match: (frame: ServerFrame) => boolean) => Promise<ServerFrame>;
} {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/rt`);
  const frames: ServerFrame[] = [];
  const watchers: (() => void)[] = [];
  ws.addEventListener("message", (event) => {
    const frame = decodeServerFrame(String(event.data));
    if (frame !== undefined) frames.push(frame);
    for (const watcher of [...watchers]) watcher();
  });
  const ready = new Promise<void>((resolve, reject) => {
    ws.addEventListener("open", () => resolve());
    ws.addEventListener("error", () => reject(new Error("the socket failed to open")));
  });
  return {
    frames,
    ws,
    ready,
    until: (match) =>
      new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("no matching frame")), 3000);
        const check = (): void => {
          const found = frames.find(match);
          if (found === undefined) return;
          clearTimeout(timer);
          watchers.splice(watchers.indexOf(check), 1);
          resolve(found);
        };
        watchers.push(check);
        check();
      }),
  };
}

const rules = {
  "chat:{room}": {
    subscribe: true,
    publish: ({ identity }: { identity: { user?: string } }) => identity.user !== undefined,
    presence: true,
    handlers: { shout: ({ data }: { data: unknown }) => String(data).toUpperCase() },
  },
};

const authenticate = {
  authenticate: (event: { request: Request }) => {
    const user =
      event.request.headers.get("x-user") ?? new URL(event.request.url).searchParams.get("user");
    return user === null ? {} : { user };
  },
};

describe("the hub in a Dusk server", () => {
  it("[RT-DUSK-01] serves WebSocket clients: subscribe, publish with ack, receive, request and reply", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub, {
      authenticate: () => ({ user: "ann" }),
    });
    const a = socket(port);
    const b = socket(port);
    await Promise.all([a.ready, b.ready]);
    await a.until((frame) => frame.t === "ready");
    a.ws.send(JSON.stringify({ t: "sub", n: 1, c: "chat:lobby" }));
    b.ws.send(JSON.stringify({ t: "sub", n: 1, c: "chat:lobby" }));
    await Promise.all([a.until((f) => f.t === "subbed"), b.until((f) => f.t === "subbed")]);
    a.ws.send(JSON.stringify({ t: "pub", c: "chat:lobby", e: "message", d: { text: "hi" }, n: 2 }));
    const received = await b.until((frame) => frame.t === "msg");
    expect(received).toMatchObject({
      c: "chat:lobby",
      s: 1,
      e: "message",
      d: { text: "hi" },
      u: "ann",
    });
    expect(await a.until((frame) => frame.t === "ack")).toEqual({ t: "ack", n: 2, s: 1 });
    a.ws.close();
    b.ws.close();
  });

  it("[RT-DUSK-02] refuses an upgrade the authenticate hook refuses, with its status", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub, {
      authenticate: () => new Response("no session", { status: 401 }),
    });
    const ws = new WebSocket(`ws://127.0.0.1:${port}/rt`);
    const outcome = await new Promise<string>((resolve) => {
      ws.addEventListener("open", () => resolve("open"));
      ws.addEventListener("error", () => resolve("refused"));
      ws.addEventListener("close", () => resolve("refused"));
    });
    expect(outcome).toBe("refused");
  });

  it("[RT-DUSK-03] closes a binary frame with 1003", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub);
    const a = socket(port);
    await a.ready;
    const closed = new Promise<number>((resolve) =>
      a.ws.addEventListener("close", (e) => resolve(e.code)),
    );
    a.ws.send(new Uint8Array([1, 2, 3]));
    expect(await closed).toBe(1003);
  });

  it("[RT-DUSK-04] serves SSE clients: the stream declares channels, POST publishes and requests", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub, authenticate);
    const stream = await fetch(
      `http://127.0.0.1:${port}/rt?user=ann&s=${encodeURIComponent(JSON.stringify([{ c: "chat:lobby", presence: null }]))}`,
      { headers: { accept: "text/event-stream" } },
    );
    expect(stream.status).toBe(200);
    expect(stream.headers.get("content-type")).toContain("text/event-stream");
    const reader = stream.body?.getReader();
    const decoder = new TextDecoder();
    let text = "";
    const read = async (until: string): Promise<void> => {
      const stop = Date.now() + 3000;
      while (!text.includes(until) && Date.now() < stop) {
        const chunk = await reader?.read();
        if (chunk?.done) break;
        text += decoder.decode(chunk?.value);
      }
    };
    await read('"t":"subbed"');
    const ready = /event: rt\ndata: (\{"t":"ready".*\})\n/.exec(text)?.[1];
    const connectionId = (JSON.parse(ready ?? "{}") as { id?: string }).id;
    expect(connectionId).toBeDefined();
    const posted = await fetch(`http://127.0.0.1:${port}/rt/send?user=ann`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        cid: connectionId,
        frames: [
          { t: "pub", c: "chat:lobby", e: "message", d: "via post", n: 1 },
          { t: "req", n: 2, c: "chat:lobby", e: "shout", d: "quiet" },
          { t: "pres", c: "chat:lobby", s: { typing: true } },
        ],
      }),
    });
    expect(posted.status).toBe(200);
    const body = (await posted.json()) as { frames: ServerFrame[] };
    expect(body.frames).toContainEqual({ t: "ack", n: 1, s: 1 });
    expect(body.frames).toContainEqual({ t: "rep", n: 2, d: "QUIET" });
    await read("via post");
    expect(text).toContain('"d":"via post"');
    await read('"op":"set"');
    expect(text).toContain('"typing":true');
    await reader?.cancel();
  });

  it("[RT-DUSK-05][RT-R4] POST refuses another origin and an anonymous publish, and bounds the body", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub, authenticate);
    const post = (headers: Record<string, string>, body: string) =>
      fetch(`http://127.0.0.1:${port}/rt/send`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body,
      });
    const frame = JSON.stringify({ frames: [{ t: "pub", c: "chat:lobby", e: "m", d: 1, n: 1 }] });
    expect((await post({ origin: "https://evil.example" }, frame)).status).toBe(403);
    const anonymous = (await (await post({}, frame)).json()) as { frames: ServerFrame[] };
    expect(anonymous.frames[0]).toMatchObject({ t: "err", n: 1, code: "unauthorized" });
    expect((await post({}, "{")).status).toBe(400);
    expect((await post({}, JSON.stringify({ frames: [] }))).status).toBe(400);
    expect((await post({}, "x".repeat(600_000))).status).toBeGreaterThanOrEqual(400);
  });

  it("[RT-DUSK-06] hub.close closes live WebSockets with 1001 and the server stops cleanly", async () => {
    const hub = createHub({ channels: rules });
    hubs.push(hub);
    const port = await boot(hub);
    const a = socket(port);
    await a.ready;
    const closed = new Promise<number>((resolve) =>
      a.ws.addEventListener("close", (e) => resolve(e.code)),
    );
    await hub.close();
    expect(await closed).toBe(1001);
  });
});

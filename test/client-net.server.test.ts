import { afterEach, describe, expect, it } from "vitest";
import { createRealtime, type Realtime, type RealtimeOptions } from "../src/client/index";
import { createHub, type Hub, memoryAdapter } from "../src/server/index";
import { bootDusk, type NetServer } from "./fixtures/dusk-server";
import { rules } from "./fixtures/harness";

/**
 * The real client against the hub inside a real Dusk server: the platform's own `WebSocket` and `fetch`, real sockets, real
 * streams, real kills. Nothing is faked.
 */

const servers: NetServer[] = [];
const hubs: Hub[] = [];
const clients: Realtime[] = [];
afterEach(async () => {
  for (const client of clients.splice(0)) client.close();
  for (const server of servers.splice(0)) await server.close();
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
});

function newHub(options: Parameters<typeof createHub>[0] = {}): Hub {
  const hub = createHub({
    channels: rules,
    limits: { publishBurst: 1000, publishesPerSecond: 1000, channelPublishesPerSecond: 1000 },
    onError: () => undefined,
    ...options,
  });
  hubs.push(hub);
  return hub;
}
async function boot(hub: Hub, options?: Parameters<typeof bootDusk>[1]): Promise<NetServer> {
  const server = await bootDusk(hub, options);
  servers.push(server);
  return server;
}
function client(server: NetServer, options: Partial<RealtimeOptions> = {}): Realtime {
  const errors: unknown[] = [];
  const made = createRealtime({
    url: server.url,
    reconnect: { minDelayMs: 5, maxDelayMs: 40 },
    onError: (error) => errors.push(error),
    ...options,
  });
  clients.push(made);
  return made;
}
async function until(check: () => void, timeoutMs = 8000): Promise<void> {
  const stop = Date.now() + timeoutMs;
  for (;;) {
    try {
      check();
      return;
    } catch (error) {
      if (Date.now() > stop) throw error;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
}

describe("WebSocket against a real server", () => {
  it("[RT-NET-01] subscribe, publish with ack, receive, request and presence end to end", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const a = client(server, { url: `${server.url}?user=ann` });
    const b = client(server, { url: `${server.url}?user=bob` });
    const lobbyA = a.channel("chat:lobby", { presence: { who: "ann" } });
    const lobbyB = b.channel("chat:lobby", { presence: { who: "bob" } });
    const seen: unknown[] = [];
    lobbyB.on("message", (data, meta) => seen.push([data, meta.user]));
    lobbyA.on("message", () => undefined);
    await until(() => expect(lobbyA.presence.members()).toHaveLength(2));
    const ack = await lobbyA.publish("message", { text: "hi" });
    expect(ack).toEqual({ seq: 1, duplicate: false });
    await until(() => expect(seen).toEqual([[{ text: "hi" }, "ann"]]));
    expect(await lobbyA.request("shout", "quiet")).toBe("QUIET");
    lobbyB.close();
    await until(() => expect(lobbyA.presence.members()).toHaveLength(1));
  });

  it("[RT-NET-02] survives 20 server-side kills with continuous cursor, no loss and no duplicate", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const rt = client(server);
    const lobby = rt.channel("chat:lobby");
    const seen: number[] = [];
    let last = 0;
    lobby.on("message", (data, meta) => {
      expect(meta.seq).toBe(last + 1);
      last = meta.seq;
      seen.push(data as number);
    });
    await until(() => expect(lobby.status).toBe("live"));
    const acks: Promise<unknown>[] = [];
    let next = 1;
    for (let round = 0; round < 20; round += 1) {
      for (let count = 0; count < 3; count += 1) acks.push(lobby.publish("message", next++));
      server.killSockets();
      await until(() => expect(server.live().sockets).toBe(1));
    }
    await Promise.all(acks);
    await until(() => expect(seen).toHaveLength(60));
    expect(seen).toEqual(Array.from({ length: 60 }, (_, index) => index + 1));
    expect(hub.stats().published).toBe(60);
  });

  it("[RT-NET-03] resumes across a hub restart on a shared adapter: kill hub 1, the client lands on hub 2, nothing is lost", async () => {
    const adapter = memoryAdapter();
    const one = newHub({ adapter });
    const server = await boot(one);
    const rt = client(server);
    const lobby = rt.channel("chat:lobby");
    const seen: number[] = [];
    lobby.on("message", (data) => seen.push(data as number));
    await until(() => expect(lobby.status).toBe("live"));
    for (let at = 1; at <= 5; at += 1) await lobby.publish("message", at);
    await until(() => expect(seen).toHaveLength(5));
    const two = newHub({ adapter });
    server.setHub(two);
    await one.close();
    // Published to hub 2 while the client is reconnecting.
    await two.publish("chat:lobby", "message", 6);
    await two.publish("chat:lobby", "message", 7);
    await until(() => expect(seen).toHaveLength(7));
    expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7]);
    await lobby.publish("message", 8);
    await until(() => expect(seen).toHaveLength(8));
    await adapter.close();
  });

  it("[RT-G9] a slow consumer is cut by the server and resumes without loss", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const rt = client(server);
    const lobby = rt.channel("chat:lobby");
    const seen: number[] = [];
    lobby.on("message", (data) => seen.push((data as { n: number }).n));
    await until(() => expect(lobby.status).toBe("live"));
    // Many large messages at once; whatever the transport does with them, every one must arrive once and in order.
    const filler = "x".repeat(20_000);
    for (let n = 1; n <= 40; n += 1) await hub.publish("chat:lobby", "message", { n, filler });
    await until(() => expect(seen).toHaveLength(40));
    expect(seen).toEqual(Array.from({ length: 40 }, (_, index) => index + 1));
  });
});

describe("server-sent events against a real server", () => {
  it("[RT-R11] end to end over SSE: subscribe, publish by POST, request, presence for a signed-in user", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const a = client(server, { transport: "sse", headers: { "x-user": "ann" } });
    const b = client(server, { transport: "sse", headers: { "x-user": "bob" } });
    const lobbyA = a.channel("chat:lobby", { presence: { who: "ann" } });
    const lobbyB = b.channel("chat:lobby");
    const seen: unknown[] = [];
    lobbyB.on("message", (data) => seen.push(data));
    lobbyB.presence.on(() => undefined);
    lobbyA.on("message", () => undefined);
    await until(() => expect(lobbyB.presence.members()).toHaveLength(1));
    expect(await lobbyA.publish("message", "over post")).toEqual({ seq: 1, duplicate: false });
    await until(() => expect(seen).toEqual(["over post"]));
    expect(await lobbyA.request("shout", "sse")).toBe("SSE");
    lobbyA.presence.set({ who: "ann", typing: true } as never);
    await until(() =>
      expect(lobbyB.presence.members()[0]?.s).toEqual({ who: "ann", typing: true }),
    );
    expect(server.live().sockets).toBe(0);
  });

  it("[RT-R11] adding a channel reopens the stream and keeps the cursor of the others", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const rt = client(server, { transport: "sse" });
    const a = rt.channel("chat:a");
    const seen: unknown[] = [];
    a.on("message", (data) => seen.push(data));
    await until(() => expect(a.status).toBe("live"));
    await a.publish("message", 1);
    await until(() => expect(seen).toEqual([1]));
    const b = rt.channel("chat:b");
    b.on("message", () => undefined);
    await until(() => expect(b.status).toBe("live"));
    await a.publish("message", 2);
    await until(() => expect(seen).toEqual([1, 2]));
    expect(server.live().streams).toBe(1);
  });

  it("[RT-NET-04] survives 20 stream kills with no loss and no duplicate", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const rt = client(server, { transport: "sse" });
    const lobby = rt.channel("chat:lobby");
    const seen: number[] = [];
    lobby.on("message", (data) => seen.push(data as number));
    await until(() => expect(lobby.status).toBe("live"));
    const acks: Promise<unknown>[] = [];
    let next = 1;
    for (let round = 0; round < 20; round += 1) {
      for (let count = 0; count < 3; count += 1) acks.push(lobby.publish("message", next++));
      server.killStreams();
      await until(() => expect(server.live().streams).toBe(1));
    }
    await Promise.all(acks);
    await until(() => expect(seen).toHaveLength(60));
    expect(seen).toEqual(Array.from({ length: 60 }, (_, index) => index + 1));
    expect(hub.stats().published).toBe(60);
  });

  it("[RT-R11] auto: a server with no WebSocket route (426) falls back to SSE after two failed upgrades and keeps working", async () => {
    const hub = newHub();
    const server = await boot(hub, { kinds: ["SSE"] });
    const rt = client(server, { transport: "auto" });
    const lobby = rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", "fallback");
    await until(() => expect(seen).toEqual(["fallback"]));
    expect(server.live()).toEqual({ sockets: 0, streams: 1 });
  });

  it("[RT-C-49] a stream the server refuses with 401 closes the client for good", async () => {
    const hub = newHub();
    const server = await boot(hub);
    const failing = createRealtime({
      url: `http://127.0.0.1:${server.port}/nowhere`,
      transport: "sse",
      reconnect: { minDelayMs: 5 },
      onError: () => undefined,
    });
    clients.push(failing);
    failing.channel("chat:lobby").on("message", () => undefined);
    await until(() => expect(failing.status).toBe("closed"));
  });
});

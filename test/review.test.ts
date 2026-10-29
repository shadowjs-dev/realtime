import { afterEach, describe, expect, it, vi } from "vitest";
import type { SseLike, WebSocketLike } from "../src/server/index";
import { type Adapter, createHub, type Hub, memoryAdapter } from "../src/server/index";
import { connectTest } from "../src/testing";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
const hubs: Hub[] = [];
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
});
const quiet = (): void => undefined;

describe("client listeners", () => {
  it("[RT-REV-1] one handler added twice holds the subscription once: its stop releases the channel", async () => {
    const r = rig();
    rigs.push(r);
    const lobby = r.rt.channel("chat:lobby");
    const handler = (): void => undefined;
    const stopFirst = lobby.on("message", handler);
    const stopSecond = lobby.on("message", handler);
    await r.until(() => expect(lobby.status).toBe("live"));
    stopFirst();
    stopSecond();
    await r.until(() => expect(lobby.status).toBe("idle"));
    expect(r.hub.stats().subscriptions).toBe(0);
  });

  it("[RT-REV-1] a stop called twice does not release another listener's hold", async () => {
    const r = rig();
    rigs.push(r);
    const lobby = r.rt.channel("chat:lobby");
    const stopA = lobby.on("message", () => undefined);
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    stopA();
    stopA();
    await Promise.resolve();
    expect(lobby.status).toBe("live");
  });
});

describe("the server-sent events transport", () => {
  it("[RT-REV-7] a POST that never answers is abandoned, so the publishes after it still go out", async () => {
    let hang = true;
    let aborted = 0;
    const r = rig({
      transport: "sse",
      ackTimeoutMs: 100,
      queueTtlMs: 2000,
      reconnect: { random: () => 0.5, minDelayMs: 10 },
      fetch: (input, init) => {
        if (init?.method === "POST" && hang) {
          hang = false;
          return new Promise<Response>((_resolve, reject) => {
            init.signal?.addEventListener("abort", () => {
              aborted += 1;
              reject(new DOMException("aborted", "AbortError"));
            });
          });
        }
        return r.fetches.fetch(input, init);
      },
    });
    rigs.push(r);
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    const first = lobby.publish("message", 1);
    const second = lobby.publish("message", 2);
    await expect(first).rejects.toMatchObject({ code: "timeout" });
    await expect(second).resolves.toMatchObject({ duplicate: false });
    expect(aborted).toBe(1);
  });
});

function sseStream(): SseLike & { readonly sent: string[]; stop(): void } {
  const control = new AbortController();
  const sent: string[] = [];
  let closed = false;
  return {
    sent,
    signal: control.signal,
    get closed() {
      return closed;
    },
    send: async (message) => {
      sent.push(message.data);
    },
    close() {
      closed = true;
      control.abort();
    },
    stop() {
      control.abort();
    },
  };
}

describe("server routes", () => {
  it("[RT-REV-2] a stream declaration is always a subscription, whatever type it names", async () => {
    const hub = createHub({
      channels: { "chat:{room}": { subscribe: true, publish: true } },
      onError: quiet,
    });
    hubs.push(hub);
    const stream = sseStream();
    const url = `http://localhost/rt?s=${encodeURIComponent(
      JSON.stringify([{ t: "pub", n: 9, c: "chat:lobby", e: "message", d: "smuggled" }]),
    )}`;
    const route = hub.sse();
    const done = route({ request: new Request(url) }, stream);
    await vi.waitFor(() => expect(stream.sent.length).toBeGreaterThan(1));
    stream.stop();
    await done;
    expect(hub.stats().published).toBe(0);
    expect(stream.sent.some((text) => text.includes('"t":"subbed"'))).toBe(true);
  });

  it("[RT-REV-3] a socket that closed while its identity was being resolved leaves no connection", async () => {
    const hub = createHub({ channels: { "chat:{room}": { subscribe: true } }, onError: quiet });
    hubs.push(hub);
    let release: (value: { user: string }) => void = () => undefined;
    const route = hub.websocket({
      authenticate: () =>
        new Promise<{ user: string }>((resolve) => {
          release = resolve;
        }),
    });
    const control = new AbortController();
    const ws: WebSocketLike = {
      data: {},
      signal: control.signal,
      send: () => true,
      close: () => undefined,
    };
    const event = { request: new Request("http://localhost/rt") };
    const opening = route.open(ws, event);
    route.close(ws);
    control.abort();
    release({ user: "ann" });
    await opening;
    expect(hub.stats().connections).toBe(0);
  });

  it("[RT-REV-4] presence changes posted over HTTP are rate limited like publishes", async () => {
    const base = memoryAdapter();
    let lists = 0;
    const counting: Adapter = {
      ...base,
      presenceList: (channel) => {
        lists += 1;
        return base.presenceList(channel);
      },
    };
    const hub = createHub({
      adapter: counting,
      channels: { "chat:{room}": { subscribe: true, presence: true } },
      limits: { publishesPerSecond: 1, publishBurst: 3 },
      onError: quiet,
    });
    hubs.push(hub);
    const post = hub.post({ authenticate: () => ({ user: "ann" }) });
    const frames = Array.from({ length: 20 }, () => ({ t: "pres", c: "chat:lobby", s: { a: 1 } }));
    const response = await post({
      request: new Request("http://localhost/rt/send", {
        method: "POST",
        body: JSON.stringify({ cid: "someone", frames }),
      }),
    });
    const body = (await response.json()) as { frames: { code?: string }[] };
    expect(lists).toBeLessThanOrEqual(3);
    expect(body.frames.filter((frame) => frame.code === "rate_limited").length).toBe(17);
  });

  it("[RT-REV-5] a POST body larger than the limit is refused without reading all of it", async () => {
    const hub = createHub({
      channels: { "chat:{room}": { subscribe: true, publish: true } },
      limits: { maxMessageBytes: 1024 },
      onError: quiet,
    });
    hubs.push(hub);
    const post = hub.post();
    let pulled = 0;
    const chunk = new Uint8Array(4096).fill(97);
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulled += chunk.length;
        if (pulled > 50_000_000) controller.close();
        else controller.enqueue(chunk);
      },
    });
    const response = await post({
      request: new Request("http://localhost/rt/send", {
        method: "POST",
        body,
        duplex: "half",
      } as RequestInit),
    });
    expect(response.status).toBe(413);
    expect(pulled).toBeLessThan(1_000_000);
  });
});

describe("hub transports", () => {
  it("[RT-REV-10] a transport whose send throws is closed, not only forgotten", () => {
    const hub = createHub({ channels: {}, onError: quiet });
    hubs.push(hub);
    const closes: [number, string][] = [];
    hub.connect({
      send: () => {
        throw new Error("broken pipe");
      },
      close: (code, reason) => {
        closes.push([code, reason]);
      },
    });
    expect(closes).toEqual([[1011, "write failed"]]);
    expect(hub.stats().connections).toBe(0);
  });
});

describe("memory adapter bounds", () => {
  it("[RT-REV-9] remembers a bounded number of idempotency keys per channel, forgetting the oldest", async () => {
    const adapter = memoryAdapter();
    const options = { history: 0, historyTtlMs: 60_000, idempotencyTtlMs: 3_600_000 };
    const entry = { event: "m", data: "1" };
    for (let at = 0; at < 70_000; at += 1)
      await adapter.publish("c", entry, { ...options, key: `k${at}` });
    // The oldest key fell out of the bound; the newest is still remembered.
    expect((await adapter.publish("c", entry, { ...options, key: "k0" })).duplicate).toBe(false);
    expect((await adapter.publish("c", entry, { ...options, key: "k69999" })).duplicate).toBe(true);
    await adapter.close();
  });
});

describe("presence heartbeat", () => {
  it("[RT-REV-6] a member set again after its entry expired keeps its state", async () => {
    vi.useFakeTimers();
    const base = memoryAdapter();
    let outage = false;
    const flaky: Adapter = {
      ...base,
      presenceTouch: async (refs, ttl) => {
        if (outage) throw new Error("down");
        return base.presenceTouch(refs, ttl);
      },
    };
    const hub = createHub({
      adapter: flaky,
      channels: { "p:{x}": { subscribe: true, presence: true } },
      limits: { presenceTtlMs: 90, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    hubs.push(hub);
    const a = connectTest(hub, { user: "ann" });
    a.send({ t: "sub", n: 1, c: "p:1", presence: { here: true } });
    await vi.advanceTimersByTimeAsync(5);
    await a.settled();
    outage = true;
    await vi.advanceTimersByTimeAsync(250);
    await base.presenceReap("p:1");
    expect(await base.presenceList("p:1")).toHaveLength(0);
    outage = false;
    await vi.advanceTimersByTimeAsync(120);
    const members = await base.presenceList("p:1");
    expect(members[0]?.s).toEqual({ here: true });
  });
});

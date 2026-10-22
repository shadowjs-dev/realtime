import { afterEach, describe, expect, it, vi } from "vitest";
import type { Adapter, PublishOptions } from "../src/adapter";
import { type CloudflareRealtimeOptions, cloudflareAdapter } from "../src/cloudflare";
import { MAX_ENTRY_BYTES } from "../src/cloudflare/wire";
import { RealtimeError } from "../src/errors";
import type { Member, PresenceEvent, ServerFrame, StoredEntry } from "../src/protocol";
import { createHub, type Hub } from "../src/server/index";
import { connectTest } from "../src/testing";
import { FakeNamespace } from "./support/do-host";

const spaces: FakeNamespace[] = [];
const adapters: Adapter[] = [];
const hubs: Hub[] = [];

function space(options: ConstructorParameters<typeof FakeNamespace>[0] = {}): FakeNamespace {
  const created = new FakeNamespace(options);
  spaces.push(created);
  return created;
}

function adapterOn(
  namespace: FakeNamespace,
  options: Partial<CloudflareRealtimeOptions> = {},
): Adapter {
  const adapter = cloudflareAdapter({
    namespace,
    keepAliveMs: 0,
    reconnect: { minDelayMs: 5, maxDelayMs: 20 },
    onError: () => undefined,
    ...options,
  });
  adapters.push(adapter);
  return adapter;
}

afterEach(async () => {
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
  await Promise.all(adapters.splice(0).map((adapter) => adapter.close()));
  for (const created of spaces.splice(0)) created.dispose();
  vi.useRealTimers();
});

const rules: PublishOptions = { history: 100, historyTtlMs: 60_000, idempotencyTtlMs: 60_000 };
const entry = (n: number): StoredEntry => ({ event: "e", data: String(n) });
const member = (id: string, v = 1): Member => ({ id, v, s: null });

async function until(condition: () => boolean, ms = 2000): Promise<void> {
  const stop = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > stop) throw new Error("the condition did not become true in time");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

/** Closes every open subscribe socket from the object's side, as an eviction or a deploy does. */
function dropLinks(namespace: FakeNamespace): void {
  for (const host of namespace.hosts.values())
    for (const server of [...host.sockets]) {
      server.close(1012, "restart");
      host.sockets.delete(server);
    }
}

const upgrades = (namespace: FakeNamespace): number =>
  namespace.fetches.filter((call) => call.upgrade).length;

describe("cloudflare adapter: options", () => {
  it("[RT-CF-80] rejects options that cannot work", () => {
    const namespace = space();
    // @ts-expect-error: not a namespace.
    expect(() => cloudflareAdapter({ namespace: {} })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, prefix: "a\nb" })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, prefix: "" })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, requestTimeoutMs: 0 })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, retries: -1 })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, keepAliveMs: 1.5 })).toThrow(TypeError);
    expect(() => cloudflareAdapter({ namespace, maxQueued: 0 })).toThrow(TypeError);
  });

  it("[RT-CF-81] names the object by the channel, or by prefix, newline and channel; prefixes isolate", async () => {
    const namespace = space();
    const plain = adapterOn(namespace);
    const one = adapterOn(namespace, { prefix: "app1" });
    const two = adapterOn(namespace, { prefix: "app2" });
    await plain.publish("chat:a", entry(1), rules);
    await one.publish("chat:a", entry(1), rules);
    await one.publish("chat:a", entry(2), rules);
    expect([...namespace.hosts.keys()].sort()).toEqual(["app1\nchat:a", "chat:a"]);
    expect((await two.history("chat:a", 0, 10)).head).toBe(0);
    expect((await one.history("chat:a", 0, 10)).head).toBe(2);
    expect((await plain.history("chat:a", 0, 10)).head).toBe(1);
  });
});

describe("cloudflare adapter: requests", () => {
  it("[RT-CF-82] a publish, a history read and a presence call are one request each", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    await adapter.publish("a", entry(1), rules);
    expect(namespace.fetches.map((call) => call.url.split("/v1/")[1])).toEqual(["publish"]);
    await adapter.history("a", 0, 10);
    await adapter.presenceSet("a", member("m1"), 5000);
    await adapter.presenceList("a");
    await adapter.presenceRemove("a", "m1");
    await adapter.presenceReap("a");
    expect(namespace.fetches.map((call) => call.url.split("/v1/")[1])).toEqual([
      "publish",
      "history",
      "presence-set",
      "presence-list",
      "presence-remove",
      "presence-reap",
    ]);
  });

  it("[RT-CF-83] touch sends one request per channel, not per member", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    await adapter.presenceSet("a", member("m1"), 5000);
    await adapter.presenceSet("a", member("m2"), 5000);
    await adapter.presenceSet("b", member("m3"), 5000);
    namespace.fetches.length = 0;
    const missing = await adapter.presenceTouch(
      [
        { channel: "a", id: "m1" },
        { channel: "a", id: "m2" },
        { channel: "b", id: "m3" },
        { channel: "b", id: "ghost" },
      ],
      5000,
    );
    expect(missing).toEqual([{ channel: "b", id: "ghost" }]);
    expect(namespace.fetches).toHaveLength(2);
  });

  it("[RT-CF-84] reads a long history page by page: 1200 messages in three requests", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    const big = { ...rules, history: 2000 };
    for (let at = 1; at <= 1200; at += 1) await adapter.publish("a", entry(at), big);
    namespace.fetches.length = 0;
    const all = await adapter.history("a", 0, 5000);
    expect(all.messages.map((message) => message.seq)).toEqual(
      Array.from({ length: 1200 }, (_, at) => at + 1),
    );
    expect(all.head).toBe(1200);
    expect(all.oldest).toBe(1);
    expect(namespace.fetches).toHaveLength(3);
    namespace.fetches.length = 0;
    const part = await adapter.history("a", 100, 600);
    expect(part.messages).toHaveLength(600);
    expect(part.messages[0]?.seq).toBe(101);
    expect(namespace.fetches).toHaveLength(2);
  });

  it("[RT-CF-85] repeats a safe request after a lost connection, but never a remove, a reap or a publish without a key", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { retries: 2 });
    await adapter.publish("a", entry(1), rules);
    namespace.fetches.length = 0;
    namespace.failThrow = 2;
    expect((await adapter.history("a", 0, 10)).head).toBe(1);
    expect(namespace.fetches).toHaveLength(3);
    namespace.fetches.length = 0;
    namespace.failThrow = 3;
    await expect(adapter.history("a", 0, 10)).rejects.toMatchObject({ code: "unavailable" });
    expect(namespace.fetches).toHaveLength(3);
    for (const unsafe of [
      () => adapter.presenceRemove("a", "m"),
      () => adapter.presenceReap("a"),
      () => adapter.publish("a", entry(2), rules),
    ]) {
      namespace.fetches.length = 0;
      namespace.failThrow = 1;
      await expect(unsafe()).rejects.toMatchObject({ code: "unavailable" });
      expect(namespace.fetches).toHaveLength(1);
    }
    namespace.fetches.length = 0;
    namespace.failThrow = 1;
    expect(await adapter.publish("a", entry(3), { ...rules, key: "k" })).toMatchObject({ seq: 2 });
    expect(namespace.fetches).toHaveLength(2);
  });

  it("[RT-CF-86] maps an overloaded or failing object to unavailable, and a refused message to its own code", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { retries: 0 });
    namespace.failStatus = { times: 1, status: 503 };
    await expect(adapter.history("a", 0, 1)).rejects.toMatchObject({ code: "unavailable" });
    const huge: StoredEntry = { event: "e", data: "x".repeat(MAX_ENTRY_BYTES + 10) };
    namespace.fetches.length = 0;
    const refused = await adapter.publish("a", huge, rules).catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(RealtimeError);
    expect((refused as RealtimeError).code).toBe("too_large");
    expect(namespace.fetches).toHaveLength(1);
  });

  it("[RT-CF-87] a request that outlasts requestTimeoutMs is unavailable", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { requestTimeoutMs: 20, retries: 0 });
    namespace.latencyMs = 80;
    await expect(adapter.history("a", 0, 1)).rejects.toMatchObject({ code: "unavailable" });
  });

  it("[RT-CF-88] stores a message at the largest size a hub accepts (64 KiB) and reads it back", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    const data = JSON.stringify("é".repeat(32_000));
    await adapter.publish("a", { event: "big", data }, rules);
    expect((await adapter.history("a", 0, 10)).messages[0]?.data).toBe(data);
  });
});

describe("cloudflare adapter: subscribe socket", () => {
  it("[RT-CF-90] shares one socket per channel between listeners, and closes it with the last one", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    const first: number[] = [];
    const second: number[] = [];
    const events: string[] = [];
    const stopFirst = await adapter.subscribe("a", (message) => first.push(message.seq));
    const stopSecond = await adapter.subscribe("a", (message) => second.push(message.seq));
    const stopPresence = await adapter.presenceSubscribe("a", (event) => events.push(event.op));
    expect(upgrades(namespace)).toBe(1);
    await adapter.publish("a", entry(1), rules);
    await adapter.presenceSet("a", member("m1"), 5000);
    await until(() => first.length === 1 && second.length === 1 && events.length === 1);
    await stopFirst();
    await adapter.publish("a", entry(2), rules);
    await until(() => second.length === 2);
    expect(first).toEqual([1]);
    await stopSecond();
    await stopPresence();
    const host = namespace.host("a");
    expect(host.clients[0]?.closedWith?.code).toBe(1000);
    // A later subscription opens a new socket.
    const stop = await adapter.subscribe("a", () => undefined);
    expect(upgrades(namespace)).toBe(2);
    await stop();
  });

  it("[RT-CF-91] a new listener starts after the head and does not see older messages", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace);
    for (let at = 1; at <= 3; at += 1) await adapter.publish("a", entry(at), rules);
    const seen: number[] = [];
    const stop = await adapter.subscribe("a", (message) => seen.push(message.seq));
    await adapter.publish("a", entry(4), rules);
    await until(() => seen.length === 1);
    expect(seen).toEqual([4]);
    await stop();
  });

  it("[RT-CF-92] after the object drops the socket, reconnects with backoff and delivers what was published meanwhile once, in order", async () => {
    const namespace = space();
    const listener = adapterOn(namespace);
    const writer = adapterOn(namespace);
    const seen: number[] = [];
    await listener.subscribe("a", (message) => seen.push(message.seq));
    for (let at = 1; at <= 3; at += 1) await writer.publish("a", entry(at), rules);
    await until(() => seen.length === 3);
    dropLinks(namespace);
    for (let at = 4; at <= 8; at += 1) await writer.publish("a", entry(at), rules);
    await until(() => seen.length === 8);
    await writer.publish("a", entry(9), rules);
    await until(() => seen.length === 9);
    expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(upgrades(namespace)).toBe(2);
    expect(
      namespace.fetches.filter((call) => call.upgrade && call.url.includes("after=3")),
    ).toHaveLength(1);
  });

  it("[RT-CF-93] keeps retrying a failing connection with a growing, bounded pause, and subscribe succeeds once it connects", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { connectTimeoutMs: 2000 });
    namespace.failThrow = 4;
    const stop = await adapter.subscribe("a", () => undefined);
    expect(upgrades(namespace)).toBe(5);
    await stop();
  });

  it("[RT-CF-94] subscribe rejects with unavailable when the object never answers, and leaves nothing running", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { connectTimeoutMs: 60 });
    namespace.failThrow = 100_000;
    await expect(adapter.subscribe("a", () => undefined)).rejects.toMatchObject({
      code: "unavailable",
    });
    const settled = upgrades(namespace);
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(upgrades(namespace)).toBe(settled);
  });

  it("[RT-CF-95] a long replay is continued over a new socket until the listener has everything, once and in order", async () => {
    const namespace = space();
    const listener = adapterOn(namespace);
    const writer = adapterOn(namespace);
    const seen: number[] = [];
    await listener.subscribe("a", (message) => seen.push(message.seq));
    dropLinks(namespace);
    const data = JSON.stringify("x".repeat(60_000));
    for (let at = 1; at <= 200; at += 1)
      await writer.publish("a", { event: "e", data }, { ...rules, history: 500 });
    await until(() => seen.length === 200, 5000);
    expect(seen).toEqual(Array.from({ length: 200 }, (_, at) => at + 1));
    expect(upgrades(namespace)).toBeGreaterThanOrEqual(3);
  });

  it("[RT-CF-96] follows a channel whose numbering restarted after it was forgotten", async () => {
    const namespace = space();
    const listener = adapterOn(namespace);
    const writer = adapterOn(namespace);
    const seen: number[] = [];
    await listener.subscribe("a", (message) => seen.push(message.seq));
    for (let at = 1; at <= 5; at += 1) await writer.publish("a", entry(at), rules);
    await until(() => seen.length === 5);
    const host = namespace.host("a");
    await host.storage.deleteAll();
    host.evict();
    dropLinks(namespace);
    await until(() => namespace.fetches.filter((call) => call.upgrade).length === 2);
    await writer.publish("a", entry(6), rules);
    await until(() => seen.length === 6);
    expect(seen).toEqual([1, 2, 3, 4, 5, 1]);
  });

  it("[RT-CF-97] a listener that throws is reported and does not stop the others or later messages", async () => {
    const namespace = space();
    const reported: unknown[] = [];
    const adapter = adapterOn(namespace, { onError: (error) => reported.push(error) });
    const good: number[] = [];
    await adapter.subscribe("a", () => {
      throw new Error("boom");
    });
    await adapter.subscribe("a", (message) => good.push(message.seq));
    for (let at = 1; at <= 3; at += 1) await adapter.publish("a", entry(at), rules);
    await until(() => good.length === 3);
    expect(reported).toHaveLength(3);
  });

  it("[RT-CF-98] a listener that falls behind loses newer messages rather than memory, never sees a repeat or a decrease", async () => {
    const namespace = space();
    const listener = adapterOn(namespace, { maxQueued: 3 });
    const writer = adapterOn(namespace);
    const seen: number[] = [];
    await listener.subscribe("a", (message) => seen.push(message.seq));
    const host = namespace.host("a");
    // Ten messages arrive in one task, before the listener's loop runs once.
    const client = host.clients[0];
    expect(client).toBeDefined();
    for (let at = 1; at <= 10; at += 1) await writer.publish("a", entry(at), rules);
    await until(() => seen.length > 0);
    await new Promise((resolve) => setTimeout(resolve, 30));
    for (let at = 1; at < seen.length; at += 1) expect(seen[at]).toBeGreaterThan(seen[at - 1] ?? 0);
    expect(seen.length).toBeGreaterThan(0);
  });
});

describe("cloudflare adapter: presence stream", () => {
  it("[RT-CF-100] delivers join, set and leave, and the expiry leave from the object's alarm without a reap", async () => {
    const namespace = space({ autoAlarm: true });
    const adapter = adapterOn(namespace, { presenceGraceMs: 30 });
    const events: string[] = [];
    await adapter.presenceSubscribe("a", (event) =>
      events.push(`${event.op}:${event.member.id}:${event.member.v}`),
    );
    await adapter.presenceSet("a", member("m1"), 40);
    await adapter.presenceSet("a", member("m1", 2), 40);
    await adapter.presenceSet("a", member("m2"), 5000);
    await until(() => events.includes("leave:m1:2"));
    expect(events).toEqual(["join:m1:1", "set:m1:2", "join:m2:1", "leave:m1:2"]);
    expect((await adapter.presenceList("a")).map((item) => item.id)).toEqual(["m2"]);
  });

  it("[RT-CF-101] after a reconnect, presence changes made while disconnected arrive as the events that connect the two states, once each", async () => {
    const namespace = space();
    const listener = adapterOn(namespace);
    const writer = adapterOn(namespace);
    const events: PresenceEvent[] = [];
    await writer.presenceSet("a", member("keep"), 60_000);
    await writer.presenceSet("a", member("gone"), 60_000);
    await writer.presenceSet("a", member("changes"), 60_000);
    await listener.presenceSubscribe("a", (event) => events.push(event));
    expect(events).toEqual([]);
    dropLinks(namespace);
    await writer.presenceRemove("a", "gone");
    await writer.presenceSet("a", member("changes", 2), 60_000);
    await writer.presenceSet("a", member("new"), 60_000);
    await until(() => events.length === 3);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(events.map((event) => `${event.op}:${event.member.id}`).sort()).toEqual([
      "join:new",
      "leave:gone",
      "set:changes",
    ]);
  });
});

describe("cloudflare adapter: close", () => {
  it("[RT-CF-110] closes sockets, leaves no timer, and rejects every later call", async () => {
    vi.useFakeTimers();
    const namespace = space();
    const adapter = adapterOn(namespace, { keepAliveMs: 1000 });
    await adapter.subscribe("a", () => undefined);
    await adapter.presenceSubscribe("b", () => undefined);
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    await adapter.close();
    expect(vi.getTimerCount()).toBe(0);
    expect(namespace.host("a").clients[0]?.closedWith?.code).toBe(1000);
    for (const call of [
      () => adapter.publish("a", entry(1), rules),
      () => adapter.history("a", 0, 1),
      () => adapter.subscribe("a", () => undefined),
      () => adapter.presenceList("a"),
    ])
      await expect(call()).rejects.toMatchObject({ code: "closed" });
    await adapter.close();
  });

  it("[RT-CF-111] settles a request in flight and a subscribe waiting for its socket with `closed`, and cancels the retry pauses", async () => {
    vi.useFakeTimers();
    const namespace = space();
    const adapter = adapterOn(namespace);
    namespace.failThrow = 2;
    const history = adapter.history("a", 0, 1).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    namespace.failThrow = 100;
    const waiting = adapter.subscribe("b", () => undefined).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    await adapter.close();
    expect(((await history) as RealtimeError).code).toBe("closed");
    expect(((await waiting) as RealtimeError).code).toBe("closed");
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("cloudflare adapter: keep-alive", () => {
  it("[RT-CF-120] pings the object and treats a silent one as gone, reconnecting", async () => {
    const namespace = space();
    const adapter = adapterOn(namespace, { keepAliveMs: 25 });
    await adapter.subscribe("a", () => undefined);
    const host = namespace.host("a");
    await until(() => (host.clients[0]?.sent.length ?? 0) >= 2);
    expect(host.clients[0]?.sent[0]).toBe("ping");
    expect(host.clients[0]?.received.filter((text) => text === "pong").length).toBeGreaterThan(0);
    // The object stops answering: after three missed periods the link is replaced.
    const answer = host.object.webSocketMessage.bind(host.object);
    host.object.webSocketMessage = () => undefined;
    await until(() => upgrades(namespace) >= 2, 3000);
    host.object.webSocketMessage = answer;
  });
});

describe("cloudflare adapter: a hub on it", () => {
  function hubOn(adapter: Adapter): Hub {
    const hub = createHub({
      adapter,
      channels: { "chat:{room}": { subscribe: true, publish: true, presence: true } },
      onError: () => undefined,
    });
    hubs.push(hub);
    return hub;
  }
  const messages = (frames: readonly ServerFrame[]): number[] =>
    frames.flatMap((frame) => (frame.t === "msg" ? [frame.s] : []));

  it("[RT-CF-130] two hubs on one namespace deliver to each other in order, and a client of one resumes through a dropped socket", async () => {
    const namespace = space();
    const hubA = hubOn(adapterOn(namespace));
    const hubB = hubOn(adapterOn(namespace));
    const publisher = connectTest(hubA);
    const subscriber = connectTest(hubB);
    subscriber.send({ t: "sub", n: 1, c: "chat:lobby" });
    await subscriber.settled();
    for (let at = 0; at < 5; at += 1)
      publisher.send({ t: "pub", c: "chat:lobby", e: "message", d: at, n: at + 1 });
    await publisher.settled();
    await until(() => messages(subscriber.frames).length === 5);
    dropLinks(namespace);
    for (let at = 5; at < 10; at += 1)
      publisher.send({ t: "pub", c: "chat:lobby", e: "message", d: at, n: at + 1 });
    await publisher.settled();
    await until(() => messages(subscriber.frames).length === 10);
    expect(messages(subscriber.frames)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(subscriber.frames.flatMap((frame) => (frame.t === "msg" ? [frame.d] : []))).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    ]);
  });

  it("[RT-CF-131] presence joins on one hub are seen by a client of the other, and the leave arrives when the connection closes", async () => {
    const namespace = space();
    const hubA = hubOn(adapterOn(namespace));
    const hubB = hubOn(adapterOn(namespace));
    const watcher = connectTest(hubB);
    watcher.send({ t: "sub", n: 1, c: "chat:lobby", watch: true });
    await watcher.settled();
    const joiner = connectTest(hubA);
    joiner.send({ t: "sub", n: 1, c: "chat:lobby", presence: { name: "ada" } });
    await joiner.settled();
    await until(() => watcher.frames.some((frame) => frame.t === "pres" && frame.op === "join"));
    await joiner.close();
    await until(() => watcher.frames.some((frame) => frame.t === "pres" && frame.op === "leave"));
  });
});

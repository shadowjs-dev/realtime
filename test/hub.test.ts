import { afterEach, describe, expect, it } from "vitest";
import type { ServerFrame, StoredMessage } from "../src/index";
import { type Adapter, createHub, type Hub, memoryAdapter } from "../src/server/index";
import { connectTest, type TestConnection } from "../src/testing";

const open: Hub[] = [];
function hubOf(options: Parameters<typeof createHub>[0] = {}): Hub {
  const hub = createHub({
    channels: { "chat:{room}": { subscribe: true, publish: true, presence: true } },
    onError: () => undefined,
    ...options,
  });
  open.push(hub);
  return hub;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((hub) => hub.close()));
});

const msgs = (client: TestConnection, channel = "chat:lobby") =>
  client.frames.filter(
    (frame): frame is Extract<ServerFrame, { t: "msg" }> =>
      frame.t === "msg" && frame.c === channel,
  );
const errs = (client: TestConnection) =>
  client.frames.filter((frame): frame is Extract<ServerFrame, { t: "err" }> => frame.t === "err");

describe("subscribe and publish", () => {
  it("[RT-G1] numbers messages 1, 2, 3 without a gap and delivers them in order", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    const b = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    for (let at = 0; at < 5; at += 1)
      b.send({ t: "pub", c: "chat:lobby", e: "message", d: at, n: at + 1 });
    await b.settled();
    expect(msgs(a).map((frame) => frame.s)).toEqual([1, 2, 3, 4, 5]);
    expect(msgs(a).map((frame) => frame.d)).toEqual([0, 1, 2, 3, 4]);
  });

  it("[RT-G5] acks a publish with the seq the subscribers see", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    a.send({ t: "pub", c: "chat:lobby", e: "message", d: "x", n: 2 });
    await a.settled();
    const ack = a.frames.find((frame) => frame.t === "ack");
    expect(ack).toEqual({ t: "ack", n: 2, s: 1 });
    expect(msgs(a)[0]?.s).toBe(1);
  });

  it("[RT-R7] does not echo to a subscription that asked for echo:false but still acks", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", echo: false });
    a.send({ t: "pub", c: "chat:lobby", e: "message", d: "x", n: 2 });
    await a.settled();
    expect(msgs(a)).toHaveLength(0);
    expect(a.frames.some((frame) => frame.t === "ack")).toBe(true);
  });

  it("[RT-G7] applies a publish with the same idempotency key once", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    a.send({ t: "pub", c: "chat:lobby", e: "message", d: "x", n: 2, k: "c1:1" });
    a.send({ t: "pub", c: "chat:lobby", e: "message", d: "x", n: 3, k: "c1:1" });
    await a.settled();
    expect(msgs(a)).toHaveLength(1);
    expect(a.frames.filter((frame) => frame.t === "ack")).toEqual([
      { t: "ack", n: 2, s: 1 },
      { t: "ack", n: 3, s: 1, dup: true },
    ]);
  });

  it("[RT-G6] keeps a publisher's order across awaited rule checks", async () => {
    const hub = hubOf({
      channels: {
        "slow:{id}": {
          subscribe: true,
          publish: async () => {
            await new Promise((resolve) => setTimeout(resolve, 5));
            return true;
          },
        },
      },
    });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "slow:1" });
    for (let at = 0; at < 4; at += 1) a.send({ t: "pub", c: "slow:1", e: "e", d: at });
    await a.settled();
    expect(msgs(a, "slow:1").map((frame) => frame.d)).toEqual([0, 1, 2, 3]);
  });
});

describe("authorization", () => {
  it("[RT-R3] every rule decision sees the identity the connection was opened with", async () => {
    const seen: unknown[] = [];
    const who = { user: "u1", role: "member" };
    const hub = hubOf({
      channels: {
        "r3:{x}": {
          subscribe: ({ identity }) => {
            seen.push(identity);
            return true;
          },
          publish: ({ identity }) => {
            seen.push(identity);
            return true;
          },
        },
      },
    });
    const a = connectTest(hub, who);
    a.send({ t: "sub", n: 1, c: "r3:a" });
    a.send({ t: "pub", c: "r3:a", e: "m", d: 1 });
    await a.settled();
    expect(seen).toEqual([who, who]);
  });

  it("[RT-R1] refuses a channel no rule matches, and so closed by default", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "secret:1" });
    a.send({ t: "pub", c: "secret:1", e: "x", d: 1, n: 2 });
    await a.settled();
    expect(errs(a).map((frame) => [frame.n, frame.code])).toEqual([
      [1, "unauthorized"],
      [2, "unauthorized"],
    ]);
  });

  it("[RT-R1] answers forbidden for a signed-in user a predicate refuses, unauthorized for an anonymous one", async () => {
    const hub = hubOf({
      channels: { "admin:{x}": { subscribe: ({ identity }) => identity["role"] === "admin" } },
    });
    const user = connectTest(hub, { user: "u1", role: "member" });
    const anon = connectTest(hub);
    user.send({ t: "sub", n: 1, c: "admin:a" });
    anon.send({ t: "sub", n: 1, c: "admin:a" });
    await Promise.all([user.settled(), anon.settled()]);
    expect(errs(user)[0]?.code).toBe("forbidden");
    expect(errs(anon)[0]?.code).toBe("unauthorized");
  });

  it("[RT-R1] a predicate that throws refuses with unavailable and reports it", async () => {
    const reported: unknown[] = [];
    const hub = hubOf({
      channels: {
        "boom:{x}": {
          subscribe: () => {
            throw new Error("db down");
          },
        },
      },
      onError: (error) => reported.push(error),
    });
    const a = connectTest(hub, { user: "u" });
    a.send({ t: "sub", n: 1, c: "boom:1" });
    await a.settled();
    expect(errs(a)[0]).toMatchObject({ n: 1, code: "unavailable" });
    expect(reported).toHaveLength(1);
  });

  it("[RT-R2] captures params and the first matching rule wins", async () => {
    const seen: string[] = [];
    const hub = hubOf({
      channels: {
        "user:{id}:inbox": {
          subscribe: ({ params, identity }) => {
            seen.push(params["id"] ?? "");
            return identity.user === params["id"];
          },
        },
        "user:{...rest}": { subscribe: false },
      },
    });
    const owner = connectTest(hub, { user: "42" });
    const other = connectTest(hub, { user: "7" });
    owner.send({ t: "sub", n: 1, c: "user:42:inbox" });
    other.send({ t: "sub", n: 1, c: "user:42:inbox" });
    owner.send({ t: "sub", n: 2, c: "user:42:private:x" });
    await Promise.all([owner.settled(), other.settled()]);
    expect(owner.frames.some((frame) => frame.t === "subbed" && frame.c === "user:42:inbox")).toBe(
      true,
    );
    expect(errs(other)[0]?.code).toBe("forbidden");
    expect(errs(owner)[0]).toMatchObject({ n: 2, code: "forbidden" });
    expect(seen).toEqual(["42", "42"]);
  });

  it("[RT-G12] revoke stops delivery and says why", async () => {
    const hub = hubOf();
    const a = connectTest(hub, { user: "u1" });
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    expect(hub.revoke({ user: "u1", channel: "chat:lobby" })).toBe(1);
    await hub.publish("chat:lobby", "message", "late");
    expect(a.frames.find((frame) => frame.t === "unsubbed")).toEqual({
      t: "unsubbed",
      c: "chat:lobby",
      why: "revoked",
    });
    expect(msgs(a)).toHaveLength(0);
  });
});

describe("resume", () => {
  it("[RT-G4] replays exactly the messages after `after`, then goes live without loss or duplicate", async () => {
    const hub = hubOf();
    for (let at = 1; at <= 10; at += 1) await hub.publish("chat:lobby", "m", at);
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", after: 6 });
    await a.settled();
    await hub.publish("chat:lobby", "m", 11);
    expect(msgs(a).map((frame) => frame.s)).toEqual([7, 8, 9, 10, 11]);
    expect(msgs(a).map((frame) => frame.r)).toEqual([1, 1, 1, 1, undefined]);
    expect(a.frames.find((frame) => frame.t === "subbed")).toEqual({
      t: "subbed",
      n: 1,
      c: "chat:lobby",
      head: 10,
    });
  });

  it("[RT-G4][RT-R8] reports a gap when retention no longer covers `after`, and replays what remains", async () => {
    const hub = hubOf({
      channels: { "chat:{room}": { subscribe: true, publish: true, history: 3 } },
    });
    for (let at = 1; at <= 10; at += 1) await hub.publish("chat:lobby", "m", at);
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", after: 2 });
    await a.settled();
    expect(a.frames.find((frame) => frame.t === "subbed")).toMatchObject({ gap: true, head: 10 });
    expect(msgs(a).map((frame) => frame.s)).toEqual([8, 9, 10]);
  });

  it("[RT-G4] reports a reset when `after` is above the head, and replays nothing", async () => {
    const hub = hubOf();
    await hub.publish("chat:lobby", "m", 1);
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", after: 99 });
    await a.settled();
    expect(a.frames.find((frame) => frame.t === "subbed")).toMatchObject({ reset: true, head: 1 });
    await hub.publish("chat:lobby", "m", 2);
    expect(msgs(a).map((frame) => frame.s)).toEqual([2]);
  });

  it("[RT-G4] loses nothing when messages are published while a long replay runs", async () => {
    const hub = hubOf({
      channels: { "chat:{room}": { subscribe: true, publish: true, history: 2000 } },
    });
    for (let at = 1; at <= 1200; at += 1) await hub.publish("chat:lobby", "m", at);
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", after: 0 });
    const live = (async () => {
      for (let at = 1201; at <= 1300; at += 1) await hub.publish("chat:lobby", "m", at);
    })();
    await Promise.all([a.settled(), live]);
    await a.settled();
    await new Promise((resolve) => setTimeout(resolve, 20));
    const seqs = msgs(a).map((frame) => frame.s);
    expect(seqs).toEqual(Array.from({ length: 1300 }, (_, at) => at + 1));
  });

  it("[RT-G3] fetches a message the live listener skipped from history instead of losing it", async () => {
    const base = memoryAdapter();
    const lossy: Adapter = {
      ...base,
      subscribe: (channel, listener) =>
        base.subscribe(channel, (message) => {
          if (message.seq !== 2) listener(message);
        }),
    };
    const hub = hubOf({ adapter: lossy });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    for (let at = 1; at <= 4; at += 1) await hub.publish("chat:lobby", "m", at);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(msgs(a).map((frame) => frame.s)).toEqual([1, 2, 3, 4]);
    expect(a.frames.some((frame) => frame.t === "unsubbed")).toBe(false);
  });

  it("[RT-G3] removes a subscription with unsubbed gap when the skipped message is no longer retained", async () => {
    const base = memoryAdapter();
    const lossy: Adapter = {
      ...base,
      subscribe: (channel, listener) =>
        base.subscribe(channel, (message) => {
          if (message.seq !== 2) listener(message);
        }),
    };
    const hub = hubOf({
      adapter: lossy,
      channels: { "chat:{room}": { subscribe: true, publish: true, history: 1 } },
    });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    for (let at = 1; at <= 4; at += 1) await hub.publish("chat:lobby", "m", at);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(a.frames.find((frame) => frame.t === "unsubbed")).toEqual({
      t: "unsubbed",
      c: "chat:lobby",
      why: "gap",
    });
  });
});

describe("numbering restart", () => {
  it("[RT-G4] a subscriber whose channel restarted its numbering is removed with unsubbed reset; a repeated old message is only dropped", async () => {
    const base = memoryAdapter();
    let listener: ((message: StoredMessage) => void) | undefined;
    const spy: Adapter = {
      ...base,
      subscribe: async (channel, next) => {
        listener = next;
        return base.subscribe(channel, next);
      },
    };
    const hub = hubOf({ adapter: spy });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    for (let at = 1; at <= 5; at += 1) await hub.publish("chat:lobby", "m", at);
    const last = (await base.history("chat:lobby", 4, 1)).messages[0] as StoredMessage;
    listener?.(last);
    expect(a.frames.some((frame) => frame.t === "unsubbed")).toBe(false);
    listener?.({ seq: 1, event: "m", data: "99", at: last.at + 5000 });
    expect(a.frames.find((frame) => frame.t === "unsubbed")).toEqual({
      t: "unsubbed",
      c: "chat:lobby",
      why: "reset",
    });
    a.send({ t: "sub", n: 2, c: "chat:lobby", after: 5 });
    await a.settled();
    expect(a.frames.filter((frame) => frame.t === "subbed").at(-1)).toMatchObject({ head: 5 });
  });
});

describe("acting for a connection", () => {
  it("[RT-G7][RT-R7] a request acting for an open connection of the same user is that publisher: echo:false suppresses it; another user's id is not honored", async () => {
    const hub = hubOf();
    const stream = connectTest(hub, { user: "ann" });
    stream.send({ t: "sub", n: 1, c: "chat:lobby", echo: false });
    await stream.settled();
    const seen: string[] = [];
    const post = (identity: { user: string }) =>
      hub.connect(
        {
          send: (frame) => {
            seen.push(frame);
            return true;
          },
          close: () => undefined,
        },
        identity,
        {
          stateless: true,
          actingFor: stream.id,
        },
      );
    const mine = post({ user: "ann" });
    mine.receive('{"t":"pub","c":"chat:lobby","e":"m","d":1,"n":1}');
    await mine.settled();
    const stranger = post({ user: "mallory" });
    stranger.receive('{"t":"pub","c":"chat:lobby","e":"m","d":2,"n":1}');
    await stranger.settled();
    // ann's own post is hers (suppressed on her echo:false stream); mallory's post cannot pose as ann's stream.
    expect(msgs(stream).map((frame) => frame.d)).toEqual([2]);
    // The stream and the stranger's own connection; the acting connection did not replace its owner in the table.
    expect(hub.stats().connections).toBe(2);
    mine.closed();
    stranger.closed();
    await Promise.all([mine.done, stranger.done]);
    expect(hub.stats().connections).toBe(1);
  });
});

describe("requests", () => {
  const rules = {
    "rpc:{id}": {
      subscribe: true,
      handlers: {
        double: ({ data }: { data: unknown }) => (data as number) * 2,
        slow: async ({ signal }: { signal: AbortSignal }) =>
          new Promise<never>((_resolve, reject) =>
            signal.addEventListener("abort", () => reject(signal.reason)),
          ),
        fail: () => {
          throw new Error("secret internals");
        },
      },
    },
  };

  it("[RT-G10] answers a request once with the reply", async () => {
    const hub = hubOf({ channels: rules });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "rpc:1" });
    a.send({ t: "req", n: 2, c: "rpc:1", e: "double", d: 21 });
    await a.settled();
    expect(a.frames.filter((frame) => frame.t === "rep")).toEqual([{ t: "rep", n: 2, d: 42 }]);
  });

  it("[RT-G10] times out a handler that never answers, and aborts its signal", async () => {
    const hub = hubOf({ channels: rules, limits: { requestTimeoutMs: 30 } });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "rpc:1" });
    a.send({ t: "req", n: 2, c: "rpc:1", e: "slow", d: null });
    await a.settled();
    expect(errs(a)[0]).toMatchObject({ n: 2, code: "timeout" });
  });

  it("[RT-G10] hides the message of a handler failure that is not a RealtimeError", async () => {
    const hub = hubOf({ channels: rules });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "rpc:1" });
    a.send({ t: "req", n: 2, c: "rpc:1", e: "fail", d: null });
    await a.settled();
    expect(errs(a)[0]).toEqual({ t: "err", n: 2, c: "rpc:1", code: "handler_error" });
  });

  it("[RT-G10] refuses a request on a channel the connection has not joined, and an unknown event", async () => {
    const hub = hubOf({ channels: rules });
    const a = connectTest(hub);
    a.send({ t: "req", n: 1, c: "rpc:1", e: "double", d: 1 });
    a.send({ t: "sub", n: 2, c: "rpc:1" });
    a.send({ t: "req", n: 3, c: "rpc:1", e: "nothing", d: 1 });
    await a.settled();
    expect(errs(a).map((frame) => [frame.n, frame.code])).toEqual([
      [1, "not_subscribed"],
      [3, "no_handler"],
    ]);
  });
});

describe("presence", () => {
  it("[RT-G11] syncs the list on subscribe and announces join, set and leave in order", async () => {
    const hub = hubOf();
    const a = connectTest(hub, { user: "ann" });
    const b = connectTest(hub, { user: "bob" });
    a.send({ t: "sub", n: 1, c: "chat:lobby", presence: { typing: false } });
    await a.settled();
    b.send({ t: "sub", n: 1, c: "chat:lobby", presence: { typing: false } });
    await b.settled();
    a.send({ t: "pres", c: "chat:lobby", s: { typing: true } });
    await a.settled();
    await b.close();
    await new Promise((resolve) => setTimeout(resolve, 5));
    const ops = a.frames
      .filter((frame) => frame.t === "pres")
      .map((frame) => (frame.t === "pres" ? frame.op : ""));
    expect(ops).toEqual(["sync", "join", "set", "leave"]);
    expect(a.frames.filter((frame) => frame.t === "pres" && frame.op === "join")).toHaveLength(1);
    const sync = a.frames.find((frame) => frame.t === "pres" && frame.op === "sync");
    expect(sync && sync.t === "pres" ? sync.m : null).toHaveLength(1);
  });

  it("[RT-G11] removes a member when its connection drops, exactly once", async () => {
    const hub = hubOf();
    const watcher = connectTest(hub);
    const member = connectTest(hub, { user: "ann" });
    watcher.send({ t: "sub", n: 1, c: "chat:lobby", watch: true });
    member.send({ t: "sub", n: 1, c: "chat:lobby", presence: null });
    await Promise.all([watcher.settled(), member.settled()]);
    await member.close();
    await member.close();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(
      watcher.frames.filter((frame) => frame.t === "pres" && frame.op === "leave"),
    ).toHaveLength(1);
  });

  it("[RT-R5] refuses a presence state larger than the limit", async () => {
    const hub = hubOf({ limits: { maxStateBytes: 16 } });
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby", presence: "x".repeat(64) });
    await a.settled();
    expect(errs(a)[0]).toMatchObject({ n: 1, code: "too_large" });
  });
});

describe("limits and isolation", () => {
  it("[RT-R5] limits channels per connection", async () => {
    const hub = hubOf({ limits: { maxChannelsPerConnection: 2 } });
    const a = connectTest(hub);
    for (const room of ["a", "b", "c"]) a.send({ t: "sub", n: 1, c: `chat:${room}` });
    await a.settled();
    expect(errs(a)).toHaveLength(1);
    expect(errs(a)[0]?.code).toBe("limit");
  });

  it("[RT-R5] rate limits publishes, keeps the connection, and closes it after repeated abuse", async () => {
    const hub = hubOf({ limits: { publishesPerSecond: 1, publishBurst: 2 } });
    const a = connectTest(hub);
    for (let at = 0; at < 60; at += 1)
      a.send({ t: "pub", c: "chat:lobby", e: "m", d: at, n: at + 1 });
    await a.settled();
    expect(errs(a)[0]?.code).toBe("rate_limited");
    expect(a.closedWith?.code).toBe(1008);
  });

  it("[RT-R5] refuses a frame larger than the limit without closing", async () => {
    const hub = hubOf({ limits: { maxMessageBytes: 128 } });
    const a = connectTest(hub);
    a.send(JSON.stringify({ t: "pub", c: "chat:lobby", e: "m", d: "x".repeat(500), n: 1 }));
    await a.settled();
    expect(errs(a)[0]?.code).toBe("too_large");
    expect(a.closedWith).toBeUndefined();
  });

  it("[RT-R6] refuses malformed frames and closes after ten in a minute", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    for (const bad of ["{", "[]", "null", '{"t":"nope"}', '{"t":"sub","n":1,"c":"has space"}'])
      a.send(bad);
    await a.settled();
    expect(errs(a).map((frame) => frame.code)).toEqual([
      "bad_frame",
      "bad_frame",
      "bad_frame",
      "bad_frame",
      "bad_channel",
    ]);
    for (let at = 0; at < 10; at += 1) a.send("{");
    await a.settled();
    expect(a.closedWith?.code).toBe(1008);
  });

  it("[RT-G9] closes a slow consumer with 1013 and keeps serving the others", async () => {
    const hub = hubOf({ limits: { maxBufferedBytes: 64 } });
    const frames: string[] = [];
    let closed: number | undefined;
    const slow = hub.connect({
      send: (frame) => {
        frames.push(frame);
        return true;
      },
      close: (code) => {
        closed = code;
      },
      buffered: () => 10_000,
    });
    const fast = connectTest(hub);
    slow.receive('{"t":"sub","n":1,"c":"chat:lobby"}');
    fast.send({ t: "sub", n: 1, c: "chat:lobby" });
    await Promise.all([slow.settled(), fast.settled()]);
    await hub.publish("chat:lobby", "m", "hello");
    expect(closed).toBe(1013);
    expect(msgs(fast)).toHaveLength(1);
    expect(hub.stats().slowConsumers).toBeGreaterThan(0);
  });

  it("[RT-G13] a transport that throws on send does not disturb other subscribers", async () => {
    const hub = hubOf();
    const bad = hub.connect({
      send: () => {
        throw new Error("socket exploded");
      },
      close: () => undefined,
    });
    const fine = connectTest(hub);
    fine.send({ t: "sub", n: 1, c: "chat:lobby" });
    bad.receive('{"t":"sub","n":1,"c":"chat:lobby"}');
    await Promise.all([fine.settled(), bad.settled(), bad.done]);
    await hub.publish("chat:lobby", "m", 1);
    expect(msgs(fine)).toHaveLength(1);
    expect(hub.stats().connections).toBe(1);
  });

  it("[RT-G14] close sends bye 1001, empties presence and settles pending requests", async () => {
    const hub = hubOf({
      channels: {
        "rpc:{id}": {
          subscribe: true,
          presence: true,
          handlers: {
            hang: ({ signal }) =>
              new Promise((_r, reject) =>
                signal.addEventListener("abort", () => reject(signal.reason)),
              ),
          },
        },
      },
    });
    const a = connectTest(hub, { user: "u" });
    a.send({ t: "sub", n: 1, c: "rpc:1", presence: null });
    a.send({ t: "req", n: 2, c: "rpc:1", e: "hang", d: null });
    // The request never answers, so the connection is not settled: give the frames time to be handled instead.
    await new Promise((resolve) => setTimeout(resolve, 20));
    await hub.close();
    expect(a.frames.some((frame) => frame.t === "bye" && frame.code === 1001)).toBe(true);
    expect(a.closedWith?.code).toBe(1001);
    expect(hub.stats().connections).toBe(0);
  });
});

describe("server publish", () => {
  it("[RT-G1] hub.publish reaches subscribers and validates its input", async () => {
    const hub = hubOf();
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "chat:lobby" });
    await a.settled();
    const result = await hub.publish("chat:lobby", "news", { ok: true });
    expect(result).toEqual({ seq: 1, duplicate: false });
    expect(msgs(a)[0]).toMatchObject({ e: "news", d: { ok: true } });
    await expect(hub.publish("bad name", "x", 1)).rejects.toMatchObject({ code: "bad_channel" });
    await expect(hub.publish("chat:lobby", "bad event!", 1)).rejects.toMatchObject({
      code: "bad_frame",
    });
    await expect(hub.publish("chat:lobby", "x", () => 1)).rejects.toBeInstanceOf(TypeError);
    await expect(hub.publish("chat:lobby", "x", "y".repeat(70_000))).rejects.toMatchObject({
      code: "too_large",
    });
  });
});

import { afterEach, describe, expect, it, vi } from "vitest";
import { RealtimeError } from "../src/client/index";
import { memoryAdapter } from "../src/server/index";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
function make(...args: Parameters<typeof rig>): Rig {
  const made = rig(...args);
  rigs.push(made);
  return made;
}
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});

const firstSocket = (r: Rig) => {
  const socket = r.sockets.sockets[0];
  if (socket === undefined) throw new Error("no socket was opened");
  return socket;
};

describe("connecting", () => {
  it("[RT-C-01] opens no socket until a channel listens, and one socket serves every channel", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    r.rt.channel("chat:other");
    await Promise.resolve();
    expect(r.sockets.sockets).toHaveLength(0);
    expect(r.rt.status).toBe("connecting");
    lobby.on("message", () => undefined);
    r.rt.channel("chat:other").on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    await r.until(() => expect(r.rt.channel("chat:other").status).toBe("live"));
    expect(r.sockets.sockets).toHaveLength(1);
    expect(r.rt.status).toBe("open");
    expect(r.rt.connectionId).toBeDefined();
    expect(r.statuses).toEqual(["open"]);
  });

  it("[RT-C-02] a publish alone connects and is acknowledged, without subscribing", async () => {
    const r = make();
    const ack = await r.rt.channel("chat:lobby").publish("message", { text: "hi" });
    expect(ack).toEqual({ seq: 1, duplicate: false });
    expect(r.rt.channel("chat:lobby").status).toBe("idle");
    expect(r.hub.stats().subscriptions).toBe(0);
  });

  it("[RT-C-03] resolves a relative url against the page and swaps the scheme for a socket", async () => {
    const r = make({ url: "/rt?token=1", transport: "ws" });
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await r.until(() => expect(r.sockets.sockets).toHaveLength(1));
    expect(firstSocket(r).url).toBe("ws://localhost/rt?token=1");
  });
});

describe("delivery", () => {
  it("[RT-G2] delivers in order with the message meta, and drops a repeated or older sequence number", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: [unknown, number, boolean][] = [];
    lobby.on("message", (data, meta) => seen.push([data, meta.seq, meta.replayed]));
    await r.until(() => expect(lobby.status).toBe("live"));
    const peer = r.peer();
    for (const text of ["a", "b", "c"])
      peer.send({ t: "pub", c: "chat:lobby", e: "message", d: text });
    await r.until(() => expect(seen).toHaveLength(3));
    const socket = firstSocket(r);
    socket.inject({ t: "msg", c: "chat:lobby", s: 2, e: "message", d: "stale" });
    socket.inject({ t: "msg", c: "chat:lobby", s: 3, e: "message", d: "repeat" });
    socket.inject({ t: "msg", c: "chat:lobby", s: 4, e: "message", d: "d" });
    await r.until(() => expect(seen).toHaveLength(4));
    expect(seen).toEqual([
      ["a", 1, false],
      ["b", 2, false],
      ["c", 3, false],
      ["d", 4, false],
    ]);
    expect(lobby.lastSeq).toBe(4);
  });

  it("[RT-C-04] gives the publisher and user to the handler, and onAny sees every event", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const any: string[] = [];
    const metas: unknown[] = [];
    lobby.onAny((message) => any.push(`${message.event}:${JSON.stringify(message.data)}`));
    lobby.on("a", (_data, meta) => metas.push(meta));
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("a", 1);
    await lobby.publish("b", [2]);
    await r.until(() => expect(any).toEqual(["a:1", "b:[2]"]));
    expect(metas[0]).toMatchObject({ seq: 1, replayed: false, publisher: r.rt.connectionId });
  });

  it("[RT-R7] echo:false does not deliver the connection's own publishes, but still acknowledges", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby", { echo: false });
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(await lobby.publish("message", "mine")).toEqual({ seq: 1, duplicate: false });
    r.peer().send({ t: "pub", c: "chat:lobby", e: "message", d: "theirs" });
    await r.until(() => expect(seen).toEqual(["theirs"]));
  });

  it("[RT-C-05] a handler that throws is reported and does not stop the others or the channel", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", () => {
      throw new Error("handler bug");
    });
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", 1);
    await lobby.publish("message", 2);
    await r.until(() => expect(seen).toEqual([1, 2]));
    expect(r.errors).toHaveLength(2);
  });

  it("[RT-C-06] a handler may remove itself or another during delivery", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: string[] = [];
    const stopFirst = lobby.on("message", () => {
      seen.push("first");
      stopFirst();
    });
    lobby.on("message", () => seen.push("second"));
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", 1);
    await lobby.publish("message", 2);
    await r.until(() => expect(seen).toEqual(["first", "second", "second"]));
  });

  it("[RT-C-07] the channel unsubscribes when its last listener goes, and starts fresh when it returns", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const stop = lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", 1);
    await r.until(() => expect(lobby.lastSeq).toBe(1));
    stop();
    await r.until(() => expect(lobby.status).toBe("idle"));
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(0));
    expect(lobby.lastSeq).toBe(0);
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(seen).toEqual([]);
    expect(lobby.lastSeq).toBe(1);
  });

  it("[RT-C-08] replay:true delivers the retained history on the first subscribe, without a gap report", async () => {
    const r = make();
    const peer = r.peer();
    for (const text of ["a", "b", "c"]) peer.send({ t: "pub", c: "tiny:x", e: "message", d: text });
    await peer.settled();
    const tiny = r.rt.channel("tiny:x", { replay: true });
    const seen: [unknown, boolean][] = [];
    const gaps: unknown[] = [];
    tiny.on("message", (data, meta) => seen.push([data, meta.replayed]));
    tiny.onGap((info) => gaps.push(info));
    await r.until(() => expect(seen).toHaveLength(2));
    expect(seen).toEqual([
      ["b", true],
      ["c", true],
    ]);
    expect(gaps).toEqual([]);
  });
});

describe("resume", () => {
  it("[RT-G4] resubscribes every channel after a drop with its last sequence number and loses nothing", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const other = r.rt.channel("chat:other");
    const seen: [string, unknown, boolean][] = [];
    for (const channel of [lobby, other])
      channel.on("message", (data, meta) => seen.push([channel.name, data, meta.replayed]));
    await r.until(() => expect(other.status).toBe("live"));
    const peer = r.peer();
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 1 });
    peer.send({ t: "pub", c: "chat:other", e: "message", d: "x" });
    await r.until(() => expect(seen).toHaveLength(2));
    firstSocket(r).drop();
    await r.until(() => expect(r.rt.status).toBe("reconnecting"));
    expect(lobby.status).toBe("resuming");
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 2 });
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 3 });
    await r.until(() => expect(seen).toHaveLength(4));
    expect(seen.filter(([name]) => name === "chat:lobby")).toEqual([
      ["chat:lobby", 1, false],
      ["chat:lobby", 2, true],
      ["chat:lobby", 3, true],
    ]);
    expect(r.sockets.sockets).toHaveLength(2);
    const subs = r.sockets.sockets[1]?.sent.map(
      (text) => JSON.parse(text) as { t: string; c?: string; after?: number },
    );
    expect(subs?.find((frame) => frame.c === "chat:lobby")?.after).toBe(1);
    expect(subs?.find((frame) => frame.c === "chat:other")?.after).toBe(1);
    expect(lobby.status).toBe("live");
  });

  it("[RT-C-09] a gap after a long outage calls the gap handlers and continues from what is retained", async () => {
    const r = make();
    const tiny = r.rt.channel("tiny:x");
    const seen: unknown[] = [];
    const gaps: unknown[] = [];
    tiny.on("message", (data) => seen.push(data));
    tiny.onGap((info) => gaps.push(info));
    await r.until(() => expect(tiny.status).toBe("live"));
    firstSocket(r).drop();
    const peer = r.peer();
    for (let at = 1; at <= 6; at += 1) peer.send({ t: "pub", c: "tiny:x", e: "message", d: at });
    await peer.settled();
    await r.until(() => expect(tiny.status).toBe("live"));
    expect(gaps).toEqual([{ after: 0, head: 6, reset: false }]);
    expect(seen).toEqual([5, 6]);
    expect(tiny.lastSeq).toBe(6);
  });

  it("[RT-C-10] a reset (the log restarted below the cursor) moves the cursor to the head and reports it", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const gaps: unknown[] = [];
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    lobby.onGap((info) => gaps.push(info));
    await r.until(() => expect(lobby.status).toBe("live"));
    for (let at = 0; at < 5; at += 1) await lobby.publish("message", at);
    await r.until(() => expect(lobby.lastSeq).toBe(5));
    // A hub with a fresh log answers the next connection: the client holds 5, the head is 0.
    await r.restart();
    await r.until(() => expect(gaps).toEqual([{ after: 5, head: 0, reset: true }]));
    expect(lobby.lastSeq).toBe(0);
    await lobby.publish("message", "again");
    await r.until(() => expect(seen).toEqual([0, 1, 2, 3, 4, "again"]));
  });

  it("[RT-C-20] resumes across a hub restart on a shared adapter without losing or repeating a message", async () => {
    const adapter = memoryAdapter();
    const r = make({}, { adapter });
    const lobby = r.rt.channel("chat:lobby");
    const seen: [unknown, number][] = [];
    lobby.on("message", (data, meta) => seen.push([data, meta.seq]));
    await r.until(() => expect(lobby.status).toBe("live"));
    for (let at = 1; at <= 3; at += 1) await lobby.publish("message", at);
    await r.until(() => expect(seen).toHaveLength(3));
    const next = await r.restart(adapter);
    await next.publish("chat:lobby", "message", 4);
    await next.publish("chat:lobby", "message", 5);
    await r.until(() => expect(seen).toHaveLength(5));
    expect(seen.map(([, seq]) => seq)).toEqual([1, 2, 3, 4, 5]);
    expect(r.sockets.sockets.length).toBeGreaterThanOrEqual(2);
    await adapter.close();
  });

  it("[RT-C-11] an unsubscribe announced by the server for a gap or a reset subscribes again at once", async () => {
    for (const why of ["gap", "reset"]) {
      const r = make();
      const lobby = r.rt.channel("chat:lobby");
      lobby.on("message", () => undefined);
      await r.until(() => expect(lobby.status).toBe("live"));
      const socket = firstSocket(r);
      const before = socket.sent.length;
      socket.inject({ t: "unsubbed", c: "chat:lobby", why });
      await r.until(() => expect(socket.sent.length).toBeGreaterThan(before));
      const resent = socket.sent
        .slice(before)
        .map((text) => JSON.parse(text) as Record<string, unknown>);
      expect(resent.find((frame) => frame["t"] === "sub")).toMatchObject({
        c: "chat:lobby",
        after: 0,
      });
    }
  });

  it("[RT-G12] a revoked subscription closes the channel and reports the refusal", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    r.hub.revoke({ channel: "chat:lobby" });
    await r.until(() => expect(lobby.status).toBe("closed"));
    expect(r.errors[0]).toMatchObject({ code: "forbidden" });
    await expect(lobby.publish("message", 1)).rejects.toMatchObject({ code: "closed" });
    expect(r.rt.channel("chat:lobby")).not.toBe(lobby);
  });

  it("[RT-C-56] a transient refusal of a subscription (unavailable) is retried until it succeeds", async () => {
    let healthy = false;
    const r = make(
      { reconnect: { random: () => 0 } },
      {
        channels: {
          "flaky:{id}": {
            subscribe: () => {
              if (!healthy) throw new Error("backend down");
              return true;
            },
          },
        },
      },
    );
    const flaky = r.rt.channel("flaky:1");
    flaky.on("x", () => undefined);
    await r.until(() => expect(r.errors.length).toBeGreaterThanOrEqual(2));
    expect(r.errors[0]).toMatchObject({ code: "unavailable" });
    expect(flaky.status).not.toBe("closed");
    healthy = true;
    await r.until(() => expect(flaky.status).toBe("live"));
  });

  it("[RT-R1] a refused subscription closes the channel with the server's code", async () => {
    const r = make();
    const secret = r.rt.channel("secret:1");
    secret.on("message", () => undefined);
    await r.until(() => expect(secret.status).toBe("closed"));
    expect(r.errors[0]).toMatchObject({ name: "RealtimeError", code: "unauthorized" });
    const none = r.rt.channel("nothing:1");
    none.on("x", () => undefined);
    await r.until(() => expect(none.status).toBe("closed"));
  });
});

describe("publish", () => {
  it("[RT-G5] resolves with the sequence number the subscribers see", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seqs: number[] = [];
    lobby.on("message", (_data, meta) => seqs.push(meta.seq));
    const acks = await Promise.all([
      lobby.publish("message", 1),
      lobby.publish("message", 2),
      lobby.publish("message", 3),
    ]);
    expect(acks.map((ack) => ack.seq)).toEqual([1, 2, 3]);
    await r.until(() => expect(seqs).toEqual([1, 2, 3]));
  });

  it("[RT-C-12] rejects with a RealtimeError carrying the server's code", async () => {
    const r = make();
    const error = await r.rt
      .channel("readonly:1")
      .publish("x", 1)
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(RealtimeError);
    expect(error).toMatchObject({ code: "unauthorized" });
  });

  it("[RT-C-13] rejects an invalid event name, data that is not JSON and a bad key without sending", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    await expect(lobby.publish("bad name", 1)).rejects.toMatchObject({ code: "bad_frame" });
    const circular: Record<string, unknown> = {};
    circular["self"] = circular;
    await expect(lobby.publish("x", circular as never)).rejects.toMatchObject({
      code: "bad_frame",
    });
    await expect(lobby.publish("x", 1, { key: "has space" })).rejects.toMatchObject({
      code: "bad_frame",
    });
    expect(() => r.rt.channel("bad channel")).toThrow(RealtimeError);
    expect(r.sockets.sockets).toHaveLength(0);
  });

  it("[RT-C-14] rejects a message larger than the server accepts, without sending it", async () => {
    const r = make({}, { limits: { maxMessageBytes: 1000 } });
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("x", "small");
    await expect(lobby.publish("x", "y".repeat(2000))).rejects.toMatchObject({ code: "too_large" });
    expect(firstSocket(r).sent.filter((text) => text.includes("yyyy"))).toEqual([]);
  });

  it("[RT-C-15] ack:false resolves at once with seq 0 and the message still goes out", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    expect(await lobby.publish("message", "fire", { ack: false })).toEqual({
      seq: 0,
      duplicate: false,
    });
    await r.until(() => expect(seen).toEqual(["fire"]));
  });

  it("[RT-C-16] an acknowledgment that never comes rejects with timeout", async () => {
    const r = make({ ackTimeoutMs: 40 });
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("message", 0);
    firstSocket(r).stallInbound();
    await expect(lobby.publish("message", 1)).rejects.toMatchObject({ code: "timeout" });
  });

  it("[RT-G7] resends an unacknowledged publish after a reconnect with the same key and applies it once", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    const socket = firstSocket(r);
    socket.stallInbound();
    const pending = lobby.publish("message", "once");
    await r.until(() => expect(socket.sent.some((text) => text.includes('"once"'))).toBe(true));
    await r.until(() => expect(r.hub.stats().published).toBe(1));
    socket.drop();
    const ack = await pending;
    expect(ack.duplicate).toBe(true);
    expect(ack.seq).toBe(1);
    const keys = r.sockets.sockets
      .flatMap((item) => item.sent)
      .filter((text) => text.includes('"once"'))
      .map((text) => (JSON.parse(text) as { k: string }).k);
    expect(keys).toHaveLength(2);
    expect(keys[0]).toBe(keys[1]);
    expect(r.hub.stats().published).toBe(1);
    await r.until(() => expect(seen).toEqual(["once"]));
  });

  it("[RT-G6] keeps the order of publishes made one after another, across a reconnect", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    firstSocket(r).drop();
    const acks = Promise.all([1, 2, 3, 4].map((n) => lobby.publish("message", n)));
    await acks;
    await r.until(() => expect(seen).toEqual([1, 2, 3, 4]));
  });
});

describe("the offline queue", () => {
  it("[RT-R9] holds publishes while disconnected and sends them in order after the reconnect", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("message", 0);
    r.sockets.refuseAll();
    firstSocket(r).drop();
    await r.until(() => expect(r.rt.status).toBe("reconnecting"));
    const pending = [1, 2, 3].map((n) => lobby.publish("message", n));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(r.hub.stats().published).toBe(1);
    r.sockets.allow();
    const acks = await Promise.all(pending);
    expect(acks.map((ack) => ack.seq)).toEqual([2, 3, 4]);
  });

  it("[RT-R9] a full queue rejects the new publish with queue_full and keeps the rest", async () => {
    const r = make({ maxQueued: 3 });
    const lobby = r.rt.channel("chat:lobby");
    r.sockets.refuseAll();
    const first = [1, 2, 3].map((n) => lobby.publish("message", n));
    await expect(lobby.publish("message", 4)).rejects.toMatchObject({ code: "queue_full" });
    r.sockets.allow();
    expect((await Promise.all(first)).map((ack) => ack.seq)).toEqual([1, 2, 3]);
  });

  it("[RT-R9] a publish that waits longer than queueTtlMs rejects with timeout; an unacknowledged one reports it", async () => {
    vi.useFakeTimers();
    const r = make({ queueTtlMs: 1000 });
    const lobby = r.rt.channel("chat:lobby");
    r.sockets.refuseAll();
    const acked = lobby.publish("message", 1);
    const caught = acked.catch((error: unknown) => error);
    await lobby.publish("message", 2, { ack: false });
    await vi.advanceTimersByTimeAsync(1001);
    expect(await caught).toMatchObject({ code: "timeout" });
    expect(r.errors).toHaveLength(1);
    expect(r.errors[0]).toMatchObject({ code: "timeout" });
    r.sockets.allow();
  });

  it("[RT-R9] a burst larger than the queue is fine while connected", async () => {
    const r = make({ maxQueued: 3 }, { limits: { publishBurst: 500, publishesPerSecond: 500 } });
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("message", 0);
    const acks = await Promise.all(
      Array.from({ length: 50 }, (_, n) => lobby.publish("message", n)),
    );
    expect(acks.at(-1)?.seq).toBe(51);
  });
});

describe("requests", () => {
  it("[RT-G10] resolves with the reply, subscribing for the call and releasing the subscription", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    expect(await lobby.request("shout", "quiet")).toBe("QUIET");
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(0));
    expect(lobby.status).toBe("idle");
  });

  it("[RT-C-17] rejects with the server's code: no_handler, handler_error without internals", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    await expect(lobby.request("nothing", 1)).rejects.toMatchObject({ code: "no_handler" });
    const error = await lobby.request("boom", 1).catch((caught: unknown) => caught);
    expect(error).toMatchObject({ code: "handler_error" });
    expect(String((error as Error).message)).not.toContain("secret");
  });

  it("[RT-C-18] the call's own timeout answers timeout", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    await expect(lobby.request("slow", 1, { timeoutMs: 50 })).rejects.toMatchObject({
      code: "timeout",
    });
  });

  it("[RT-C-19] a request in flight when the connection dies rejects with closed instead of repeating", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("message", 0);
    const pending = lobby.request("slow", 1);
    const caught = pending.catch((error: unknown) => error);
    await r.until(() =>
      expect(firstSocket(r).sent.some((text) => text.includes('"req"'))).toBe(true),
    );
    firstSocket(r).drop();
    expect(await caught).toMatchObject({ code: "closed" });
  });
});

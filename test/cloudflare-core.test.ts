import { afterEach, describe, expect, it } from "vitest";
import type { PublishOptions } from "../src/adapter";
import { ChannelCore } from "../src/cloudflare/core";
import type { ChannelSocket } from "../src/cloudflare/types";
import { decodeLinkFrame, type LinkFrame } from "../src/cloudflare/wire";
import { RealtimeError } from "../src/errors";
import type { Member, StoredEntry } from "../src/protocol";
import { MemoryStorage } from "./support/do-storage";

/** A socket that records the frames it was sent. */
class Recorder implements ChannelSocket {
  readonly frames: LinkFrame[] = [];
  closed: number | undefined;
  failSends = 0;
  send(data: string): void {
    if (this.failSends > 0) {
      this.failSends -= 1;
      throw new Error("write failed");
    }
    const frame = decodeLinkFrame(data);
    if (frame === undefined) throw new Error(`undecodable frame ${data}`);
    this.frames.push(frame);
  }
  close(code?: number): void {
    this.closed = code ?? 1005;
  }
  of<T extends LinkFrame["t"]>(type: T): Extract<LinkFrame, { t: T }>[] {
    return this.frames.filter((frame): frame is Extract<LinkFrame, { t: T }> => frame.t === type);
  }
  get seqs(): number[] {
    return this.of("msg").map((frame) => frame.m.seq);
  }
  get presence(): string[] {
    return this.of("pres").map((frame) => `${frame.op}:${frame.member.id}:${frame.member.v}`);
  }
}

const clock = { t: 1_000_000 };
const now = (): number => clock.t;
const CHANNEL = "room:one";
const rules: PublishOptions = { history: 100, historyTtlMs: 60_000, idempotencyTtlMs: 60_000 };
const entry = (n: number): StoredEntry => ({ event: "e", data: String(n) });
const member = (id: string, v = 1, s: Member["s"] = null): Member => ({ id, v, s });

function make(options: { maxMembers?: number } = {}): {
  core: ChannelCore;
  storage: MemoryStorage;
} {
  const storage = new MemoryStorage();
  return { core: new ChannelCore({ storage, now, ...options }), storage };
}

/** What the runtime does when an alarm is due: it clears the alarm, then calls the handler. */
async function fire(core: ChannelCore, storage: MemoryStorage): Promise<void> {
  storage.consumeAlarm();
  await core.alarm();
}

/** A new core on the same storage: the object was evicted and restarted. */
function restart(storage: MemoryStorage): ChannelCore {
  return new ChannelCore({ storage, now });
}

afterEach(() => {
  clock.t = 1_000_000;
});

describe("channel core: numbering", () => {
  it("[RT-CF-01] numbers 200 concurrent publishes 1..200 in call order, with storage that yields between steps", async () => {
    const { core, storage } = make();
    storage.yieldEvery = 1;
    const results = await Promise.all(
      Array.from({ length: 200 }, (_, at) =>
        core.publish(CHANNEL, entry(at), { ...rules, history: 500 }),
      ),
    );
    expect(results.map((result) => result.seq)).toEqual(
      Array.from({ length: 200 }, (_, at) => at + 1),
    );
    const page = await core.history(CHANNEL, 0, 500);
    expect(page.messages.map((message) => message.data)).toEqual(
      Array.from({ length: 200 }, (_, at) => String(at)),
    );
    expect(page.head).toBe(200);
  });

  it("[RT-CF-02] continues numbering after the object restarts, and keeps history", async () => {
    const { core, storage } = make();
    for (let at = 1; at <= 5; at += 1) await core.publish(CHANNEL, entry(at), rules);
    const after = restart(storage);
    expect((await after.publish(CHANNEL, entry(6), rules)).seq).toBe(6);
    const page = await after.history(CHANNEL, 0, 100);
    expect(page.messages.map((message) => message.seq)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(page.oldest).toBe(1);
  });

  it("[RT-CF-03] a failed write assigns nothing: no seq burned, no message sent, the next publish succeeds", async () => {
    const { core, storage } = make();
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    await core.publish(CHANNEL, entry(1), rules);
    storage.failNext(1, (operation) => operation === "put");
    await expect(core.publish(CHANNEL, entry(2), rules)).rejects.toThrow("injected");
    expect((await core.publish(CHANNEL, entry(3), rules)).seq).toBe(2);
    expect(socket.seqs).toEqual([1, 2]);
    expect((await core.history(CHANNEL, 0, 10)).messages.map((message) => message.data)).toEqual([
      "1",
      "3",
    ]);
  });

  it("[RT-CF-04] stores a 64 KiB message whole, in one key, and returns it unchanged", async () => {
    const { core, storage } = make();
    const data = JSON.stringify("é".repeat(32_000));
    await core.publish(CHANNEL, { event: "big", data, publisher: "p", user: "u" }, rules);
    const page = await core.history(CHANNEL, 0, 10);
    expect(page.messages[0]?.data).toBe(data);
    expect(page.messages[0]?.publisher).toBe("p");
    expect(storage.keys("h:")).toHaveLength(1);
  });

  it("[RT-CF-05] refuses an object used for a second channel", async () => {
    const { core } = make();
    await core.publish(CHANNEL, entry(1), rules);
    await expect(core.publish("other", entry(2), rules)).rejects.toMatchObject({
      code: "conflict",
    });
    expect((await core.publish(CHANNEL, entry(2), rules)).seq).toBe(2);
  });
});

describe("channel core: retention", () => {
  it("[RT-CF-10] keeps the newest `history` messages and deletes the rest from storage", async () => {
    const { core, storage } = make();
    for (let at = 1; at <= 8; at += 1)
      await core.publish(CHANNEL, entry(at), { ...rules, history: 3 });
    const page = await core.history(CHANNEL, 0, 100);
    expect(page.messages.map((message) => message.seq)).toEqual([6, 7, 8]);
    expect(page.oldest).toBe(6);
    expect(storage.keys("h:")).toHaveLength(3);
  });

  it("[RT-CF-11] drops messages older than historyTtlMs at the next publish and keeps numbering", async () => {
    const { core, storage } = make();
    const short = { ...rules, historyTtlMs: 1000 };
    await core.publish(CHANNEL, entry(1), short);
    clock.t += 400;
    await core.publish(CHANNEL, entry(2), short);
    clock.t += 700;
    await core.publish(CHANNEL, entry(3), short);
    const page = await core.history(CHANNEL, 0, 100);
    expect(page.messages.map((message) => message.seq)).toEqual([2, 3]);
    expect(page.oldest).toBe(2);
    clock.t += 5000;
    await core.publish(CHANNEL, entry(4), short);
    expect((await core.history(CHANNEL, 0, 100)).messages.map((message) => message.seq)).toEqual([
      4,
    ]);
    expect(storage.keys("h:")).toHaveLength(1);
  });

  it("[RT-CF-12] history 0 retains nothing, still numbers, and clears what was retained", async () => {
    const { core, storage } = make();
    await core.publish(CHANNEL, entry(1), rules);
    await core.publish(CHANNEL, entry(2), rules);
    expect((await core.publish(CHANNEL, entry(3), { ...rules, history: 0 })).seq).toBe(3);
    const page = await core.history(CHANNEL, 0, 10);
    expect(page.messages).toHaveLength(0);
    expect(page.head).toBe(3);
    expect(page.oldest).toBe(0);
    expect(storage.keys("h:")).toHaveLength(0);
  });

  it("[RT-CF-13] shrinking the history of 600 messages to 5 deletes in batches the platform accepts", async () => {
    const { core, storage } = make();
    for (let at = 1; at <= 600; at += 1)
      await core.publish(CHANNEL, entry(at), { ...rules, history: 1000 });
    await core.publish(CHANNEL, entry(601), { ...rules, history: 5 });
    const page = await core.history(CHANNEL, 0, 100);
    expect(page.messages.map((message) => message.seq)).toEqual([597, 598, 599, 600, 601]);
    expect(storage.keys("h:")).toHaveLength(5);
  });

  it("[RT-CF-14] a backlog larger than one operation deletes is finished by the alarm", async () => {
    const { core, storage } = make();
    for (let at = 1; at <= 1500; at += 1)
      await core.publish(CHANNEL, entry(at), { ...rules, history: 2000 });
    await core.publish(CHANNEL, entry(1501), { ...rules, history: 0 });
    expect(storage.keys("h:").length).toBeGreaterThan(0);
    expect(storage.alarmAt).not.toBe(null);
    await fire(core, storage);
    expect(storage.keys("h:")).toHaveLength(0);
    expect(storage.alarmAt).toBe(null);
  });

  it("[RT-CF-15] the alarm trims expired history of a channel nobody publishes to, then stops", async () => {
    const { core, storage } = make();
    await core.publish(CHANNEL, entry(1), { ...rules, historyTtlMs: 5000 });
    await core.publish(CHANNEL, entry(2), { ...rules, historyTtlMs: 5000 });
    expect(storage.alarmAt).toBe(clock.t + 5000 + 1000);
    clock.t += 6001;
    await fire(core, storage);
    expect(storage.keys("h:")).toHaveLength(0);
    const page = await core.history(CHANNEL, 0, 10);
    expect(page.head).toBe(2);
    expect(page.oldest).toBe(0);
    expect(storage.alarmAt).toBe(null);
  });

  it("[RT-CF-16] history never returns an expired message, even before the alarm ran", async () => {
    const { core } = make();
    await core.publish(CHANNEL, entry(1), { ...rules, historyTtlMs: 1000 });
    clock.t += 1500;
    expect((await core.history(CHANNEL, 0, 10)).messages).toHaveLength(0);
  });

  it("[RT-CF-17] pages history: limit, after, `more`, head only, and an `after` beyond the head", async () => {
    const { core, storage } = make();
    for (let at = 1; at <= 1200; at += 1)
      await core.publish(CHANNEL, entry(at), { ...rules, history: 2000 });
    const first = await core.history(CHANNEL, 0, 5000);
    expect(first.messages).toHaveLength(512);
    expect(first.more).toBe(true);
    const second = await core.history(CHANNEL, 512, 5000);
    expect(second.messages[0]?.seq).toBe(513);
    const small = await core.history(CHANNEL, 10, 4);
    expect(small.messages.map((message) => message.seq)).toEqual([11, 12, 13, 14]);
    expect(small.more).toBe(true);
    const tail = await core.history(CHANNEL, 1195, 100);
    expect(tail.messages.map((message) => message.seq)).toEqual([1196, 1197, 1198, 1199, 1200]);
    expect(tail.more).toBe(false);
    const reads = storage.counters.calls;
    const probe = await core.history(CHANNEL, Number.MAX_SAFE_INTEGER, 0);
    expect(probe).toEqual({ messages: [], head: 1200, oldest: 1, more: false });
    expect((await core.history("room:one", 0, 0)).head).toBe(1200);
    expect(storage.counters.calls).toBe(reads);
  });
});

describe("channel core: storage cost", () => {
  it("[RT-CF-18] a steady-state publish costs 2 rows written, 1 deleted and 1 read (4 written with a key), and a quiet alarm none", async () => {
    const { core, storage } = make();
    const full = { ...rules, historyTtlMs: 300_000 };
    for (let at = 0; at < 150; at += 1) {
      clock.t += 10;
      await core.publish(CHANNEL, entry(at), full);
    }
    const before = { ...storage.counters };
    for (let at = 0; at < 100; at += 1) {
      clock.t += 10;
      await core.publish(CHANNEL, entry(at), full);
    }
    const plain = { ...storage.counters };
    expect([
      (plain.rowsWritten - before.rowsWritten) / 100,
      (plain.rowsDeleted - before.rowsDeleted) / 100,
      (plain.rowsRead - before.rowsRead) / 100,
    ]).toEqual([2, 1, 1]);
    for (let at = 0; at < 100; at += 1) {
      clock.t += 10;
      await core.publish(CHANNEL, entry(at), { ...full, key: `k${at}` });
    }
    const keyed = { ...storage.counters };
    expect((keyed.rowsWritten - plain.rowsWritten) / 100).toBe(4);
    expect(keyed.alarmsSet).toBeLessThanOrEqual(3);
  });
});

describe("channel core: idempotency", () => {
  it("[RT-CF-20] a second publish with a known key returns the first seq and stores nothing", async () => {
    const { core } = make();
    const keyed = { ...rules, key: "k1" };
    expect(await core.publish(CHANNEL, entry(1), keyed)).toEqual({ seq: 1, duplicate: false });
    expect(await core.publish(CHANNEL, entry(2), keyed)).toEqual({ seq: 1, duplicate: true });
    expect((await core.history(CHANNEL, 0, 10)).messages).toHaveLength(1);
  });

  it("[RT-CF-21] 50 racing publishes with one key produce one message", async () => {
    const { core, storage } = make();
    storage.yieldEvery = 1;
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, at) =>
        core.publish(CHANNEL, entry(at), { ...rules, key: "race" }),
      ),
    );
    expect(results.filter((result) => !result.duplicate)).toHaveLength(1);
    expect(new Set(results.map((result) => result.seq))).toEqual(new Set([1]));
  });

  it("[RT-CF-22] a key is accepted again once idempotencyTtlMs has passed", async () => {
    const { core } = make();
    const keyed = { ...rules, key: "k1", idempotencyTtlMs: 1000 };
    await core.publish(CHANNEL, entry(1), keyed);
    clock.t += 1001;
    expect(await core.publish(CHANNEL, entry(2), keyed)).toEqual({ seq: 2, duplicate: false });
  });

  it("[RT-CF-23] the alarm sweeps expired keys and their index, and keeps a key that was accepted again", async () => {
    const { core, storage } = make();
    const keyed = { ...rules, history: 0, idempotencyTtlMs: 1000 };
    await core.publish(CHANNEL, entry(1), { ...keyed, key: "old" });
    await core.publish(CHANNEL, entry(2), { ...keyed, key: "again" });
    clock.t += 1500;
    await core.publish(CHANNEL, entry(3), { ...keyed, key: "again", idempotencyTtlMs: 20_000 });
    expect(storage.keys("k:")).toEqual(["k:again", "k:old"]);
    clock.t += 5000;
    await fire(core, storage);
    expect(storage.keys("k:")).toEqual(["k:again"]);
    expect(storage.keys("x:")).toHaveLength(1);
    clock.t += 30_000;
    await fire(core, storage);
    expect(storage.keys("k:")).toHaveLength(0);
    expect(storage.keys("x:")).toHaveLength(0);
    expect(storage.alarmAt).toBe(null);
  });

  it("[RT-CF-24] remembers keys across a restart", async () => {
    const { core, storage } = make();
    await core.publish(CHANNEL, entry(1), { ...rules, key: "k" });
    expect(await restart(storage).publish(CHANNEL, entry(2), { ...rules, key: "k" })).toEqual({
      seq: 1,
      duplicate: true,
    });
  });
});

describe("channel core: presence", () => {
  it("[RT-CF-30] announces join, set and leave in order, ignores an older version, and a same-version refresh is silent", async () => {
    const { core } = make();
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    await core.presenceSet(CHANNEL, member("m1", 1), 5000, 0);
    await core.presenceSet(CHANNEL, member("m1", 1), 5000, 0);
    await core.presenceSet(CHANNEL, member("m1", 3, { x: 1 }), 5000, 0);
    await core.presenceSet(CHANNEL, member("m1", 2, "stale"), 5000, 0);
    expect(await core.presenceRemove(CHANNEL, "m1")).toBe(true);
    expect(await core.presenceRemove(CHANNEL, "m1")).toBe(false);
    expect(socket.presence).toEqual(["join:m1:1", "set:m1:3", "leave:m1:3"]);
  });

  it("[RT-CF-31] 20 concurrent removes of one member answer true once and announce one leave", async () => {
    const { core, storage } = make();
    storage.yieldEvery = 1;
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    await core.presenceSet(CHANNEL, member("m1"), 5000, 0);
    const results = await Promise.all(
      Array.from({ length: 20 }, () => core.presenceRemove(CHANNEL, "m1")),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
    expect(socket.presence).toEqual(["join:m1:1", "leave:m1:1"]);
  });

  it("[RT-CF-32] lists live members only; reap removes the expired and announces each leave once, even when racing", async () => {
    const { core, storage } = make();
    storage.yieldEvery = 1;
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    await core.presenceSet(CHANNEL, member("gone"), 1000, 0);
    await core.presenceSet(CHANNEL, member("stays"), 60_000, 0);
    clock.t += 2000;
    expect((await core.presenceList(CHANNEL)).map((item) => item.id)).toEqual(["stays"]);
    const reaped = await Promise.all([
      core.presenceReap(CHANNEL),
      core.presenceReap(CHANNEL),
      core.presenceReap(CHANNEL),
    ]);
    expect(reaped.flat().map((item) => item.id)).toEqual(["gone"]);
    expect(socket.presence).toEqual(["join:gone:1", "join:stays:1", "leave:gone:1"]);
    expect(storage.keys("p:")).toEqual(["p:stays"]);
  });

  it("[RT-CF-33] the alarm announces the leave when nobody calls reap, exactly once, and a reap that comes first wins without a second leave", async () => {
    const { core, storage } = make();
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    await core.presenceSet(CHANNEL, member("a"), 1000, 500);
    await core.presenceSet(CHANNEL, member("b"), 1000, 500);
    expect(storage.alarmAt).toBe(clock.t + 1500);
    clock.t += 1500;
    await fire(core, storage);
    expect(socket.presence.filter((event) => event.startsWith("leave"))).toEqual([
      "leave:a:1",
      "leave:b:1",
    ]);
    expect(storage.keys("p:")).toHaveLength(0);
    expect(storage.alarmAt).toBe(null);
    await fire(core, storage);
    expect(await core.presenceReap(CHANNEL)).toEqual([]);
    expect(socket.presence.filter((event) => event.startsWith("leave"))).toHaveLength(2);
    // The other order: reap first, then the alarm.
    await core.presenceSet(CHANNEL, member("c"), 1000, 500);
    clock.t += 1200;
    expect((await core.presenceReap(CHANNEL)).map((item) => item.id)).toEqual(["c"]);
    clock.t += 1000;
    await fire(core, storage);
    expect(socket.presence.filter((event) => event === "leave:c:1")).toHaveLength(1);
  });

  it("[RT-CF-34] touch extends members and reports the missing ones; a touched member survives past its first expiry", async () => {
    const { core } = make();
    await core.presenceSet(CHANNEL, member("m1"), 1000, 0);
    clock.t += 800;
    expect(await core.presenceTouch(CHANNEL, ["m1", "ghost"], 1000)).toEqual(["ghost"]);
    clock.t += 800;
    expect((await core.presenceList(CHANNEL)).map((item) => item.id)).toEqual(["m1"]);
    clock.t += 400;
    expect(await core.presenceList(CHANNEL)).toEqual([]);
  });

  it("[RT-CF-35] touching 300 members writes the storage only when the stored expiry lags by half a lifetime", async () => {
    const { core, storage } = make();
    const ids = Array.from({ length: 300 }, (_, at) => `m${at}`);
    for (const id of ids) await core.presenceSet(CHANNEL, member(id), 40_000, 0);
    const baseline = storage.counters.rowsWritten;
    clock.t += 10_000;
    await core.presenceTouch(CHANNEL, ids, 40_000);
    expect(storage.counters.rowsWritten).toBe(baseline);
    clock.t += 15_000;
    await core.presenceTouch(CHANNEL, ids, 40_000);
    expect(storage.counters.rowsWritten).toBe(baseline + 300);
    clock.t += 5000;
    await core.presenceTouch(CHANNEL, ids, 40_000);
    expect(storage.counters.rowsWritten).toBe(baseline + 300);
  });

  it("[RT-CF-36] members, versions and expiry survive a restart, and the restarted object still announces the leave", async () => {
    const { core, storage } = make();
    await core.presenceSet(CHANNEL, member("m1", 4, "state"), 1000, 100);
    const next = restart(storage);
    expect(await next.presenceList(CHANNEL)).toEqual([member("m1", 4, "state")]);
    const socket = new Recorder();
    await next.attach(socket, CHANNEL, undefined);
    expect(socket.of("snap")[0]?.members).toEqual([member("m1", 4, "state")]);
    clock.t += 1200;
    await fire(restart(storage), storage);
    await fire(next, storage);
    expect(await next.presenceList(CHANNEL)).toEqual([]);
    expect(storage.keys("p:")).toHaveLength(0);
  });

  it("[RT-CF-37] refuses members beyond the limit with `limit`, but still updates an existing one", async () => {
    const { core } = make({ maxMembers: 3 });
    for (const id of ["a", "b", "c"]) await core.presenceSet(CHANNEL, member(id), 5000, 0);
    await expect(core.presenceSet(CHANNEL, member("d"), 5000, 0)).rejects.toBeInstanceOf(
      RealtimeError,
    );
    await core.presenceSet(CHANNEL, member("a", 2), 5000, 0);
    expect((await core.presenceList(CHANNEL)).find((item) => item.id === "a")?.v).toBe(2);
  });
});

describe("channel core: the alarm", () => {
  it("[RT-CF-40] sets the alarm once for many writes, moves it earlier when something is due sooner, and clears it when nothing is left", async () => {
    const { core, storage } = make();
    await core.presenceSet(CHANNEL, member("a"), 10_000, 0);
    await core.presenceSet(CHANNEL, member("b"), 10_000, 0);
    await core.presenceSet(CHANNEL, member("c"), 10_000, 0);
    expect(storage.counters.alarmsSet).toBe(1);
    expect(storage.alarmAt).toBe(clock.t + 10_000);
    await core.presenceSet(CHANNEL, member("d"), 2000, 0);
    expect(storage.counters.alarmsSet).toBe(2);
    expect(storage.alarmAt).toBe(clock.t + 2000);
    for (const id of ["a", "b", "c", "d"]) await core.presenceRemove(CHANNEL, id);
    expect(storage.alarmAt).toBe(null);
  });

  it("[RT-CF-41] an alarm that wakes before anything expired (members were touched) moves the alarm to the new time", async () => {
    const { core, storage } = make();
    await core.presenceSet(CHANNEL, member("a"), 1000, 0);
    clock.t += 900;
    await core.presenceTouch(CHANNEL, ["a"], 1000);
    clock.t += 100;
    await fire(core, storage);
    expect(await core.presenceList(CHANNEL)).toHaveLength(1);
    expect(storage.alarmAt).toBe(clock.t + 900);
  });

  it("[RT-CF-42] the alarm is idempotent and survives a storage failure: running it again finishes the work", async () => {
    const { core, storage } = make();
    await core.presenceSet(CHANNEL, member("a"), 1000, 0);
    clock.t += 1500;
    storage.failNext(1, (operation) => operation === "transaction");
    await expect(fire(core, storage)).rejects.toThrow("injected");
    await fire(core, storage);
    expect(await core.presenceList(CHANNEL)).toEqual([]);
    expect(storage.keys("p:")).toHaveLength(0);
  });

  it("[RT-CF-43] forgets an idle channel (numbering restarts), but never one with a subscriber", async () => {
    const { core, storage } = make();
    await core.publish(CHANNEL, entry(1), { ...rules, historyTtlMs: 1000 }, 10_000);
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    clock.t += 20_000;
    await fire(core, storage);
    expect((await core.history(CHANNEL, 0, 10)).head).toBe(1);
    // Idle but watched: the alarm looks again after another idle period, it does not fire in a loop.
    expect(storage.alarmAt).toBe(clock.t + 10_000);
    core.detach(socket);
    clock.t += 10_000;
    await fire(core, storage);
    expect(storage.keys()).toEqual([]);
    expect((await core.publish(CHANNEL, entry(2), rules)).seq).toBe(1);
  });
});

describe("channel core: subscribe sockets", () => {
  it("[RT-CF-50] sends ready, the member snapshot, then live messages and presence events in order", async () => {
    const { core } = make();
    await core.publish(CHANNEL, entry(1), rules);
    await core.presenceSet(CHANNEL, member("m1"), 5000, 0);
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    expect(socket.frames.map((frame) => frame.t)).toEqual(["ready", "snap"]);
    expect(socket.of("ready")[0]).toMatchObject({ head: 1, oldest: 1 });
    await core.publish(CHANNEL, entry(2), rules);
    await core.presenceSet(CHANNEL, member("m2"), 5000, 0);
    await core.publish(CHANNEL, entry(3), rules);
    expect(socket.frames.slice(2).map((frame) => frame.t)).toEqual(["msg", "pres", "msg"]);
    expect(socket.seqs).toEqual([2, 3]);
  });

  it("[RT-CF-51] resuming with `after` replays exactly the missed messages once, then goes live, with publishes racing the attach", async () => {
    const { core, storage } = make();
    storage.yieldEvery = 1;
    for (let at = 1; at <= 10; at += 1) await core.publish(CHANNEL, entry(at), rules);
    const socket = new Recorder();
    const racing = Array.from({ length: 10 }, (_, at) =>
      core.publish(CHANNEL, entry(11 + at), rules),
    );
    const attached = core.attach(socket, CHANNEL, 6);
    await Promise.all([attached, ...racing]);
    expect(socket.seqs).toEqual(Array.from({ length: 14 }, (_, at) => at + 7));
  });

  it("[RT-CF-52] replay starts at the oldest retained message when `after` is older, and sends nothing when `after` is at or past the head", async () => {
    const { core } = make();
    for (let at = 1; at <= 10; at += 1)
      await core.publish(CHANNEL, entry(at), { ...rules, history: 4 });
    const older = new Recorder();
    await core.attach(older, CHANNEL, 2);
    expect(older.seqs).toEqual([7, 8, 9, 10]);
    const current = new Recorder();
    await core.attach(current, CHANNEL, 10);
    const ahead = new Recorder();
    await core.attach(ahead, CHANNEL, 99);
    expect(current.seqs).toEqual([]);
    expect(ahead.seqs).toEqual([]);
    expect(ahead.of("ready")[0]?.head).toBe(10);
  });

  it("[RT-CF-53] a replay too large for one attach ends with `more`, and resuming from the last message completes it", async () => {
    const { core } = make();
    const data = JSON.stringify("x".repeat(60_000));
    for (let at = 1; at <= 200; at += 1)
      await core.publish(CHANNEL, { event: "e", data }, { ...rules, history: 500 });
    const first = new Recorder();
    await core.attach(first, CHANNEL, 0);
    expect(first.of("more")).toHaveLength(1);
    const last = first.seqs[first.seqs.length - 1] ?? 0;
    expect(last).toBeLessThan(200);
    expect(first.seqs).toEqual(Array.from({ length: last }, (_, at) => at + 1));
    const second = new Recorder();
    await core.attach(second, CHANNEL, last);
    expect(second.seqs[0]).toBe(last + 1);
  });

  it("[RT-CF-54] a socket that cannot be written is closed and dropped; the others still receive", async () => {
    const { core } = make();
    const good = new Recorder();
    const bad = new Recorder();
    await core.attach(good, CHANNEL, undefined);
    await core.attach(bad, CHANNEL, undefined);
    bad.failSends = 99;
    await core.publish(CHANNEL, entry(1), rules);
    await core.publish(CHANNEL, entry(2), rules);
    expect(good.seqs).toEqual([1, 2]);
    expect(bad.closed).toBe(1011);
    expect(core.sockets).toBe(1);
  });

  it("[RT-CF-55] adopts sockets that outlived an earlier instance, and forgets one after detach", async () => {
    const { core, storage } = make();
    const socket = new Recorder();
    await core.attach(socket, CHANNEL, undefined);
    const next = restart(storage);
    next.adopt([socket]);
    await next.publish(CHANNEL, entry(1), rules);
    expect(socket.seqs).toEqual([1]);
    next.detach(socket);
    await next.publish(CHANNEL, entry(2), rules);
    expect(socket.seqs).toEqual([1]);
    expect(next.sockets).toBe(0);
  });
});

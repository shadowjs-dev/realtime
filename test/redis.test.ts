import { describe, expect, it } from "vitest";
import { RealtimeError } from "../src/errors";
import type { Json, Member, PresenceEvent, StoredMessage } from "../src/protocol";
import { redisAdapter } from "../src/redis";
import {
  HISTORY,
  KEEPALIVE,
  PRESENCE_LIST,
  PRESENCE_REAP,
  PRESENCE_REMOVE,
  PRESENCE_SET,
  PRESENCE_TOUCH,
  PUBLISH,
  sha1,
} from "../src/redis/scripts";
import { entry, RULES, sleep, until, useRedis } from "./support/fixture";

/** Retains more than any test publishes, so a recovery has everything to read. */
const RETAIN = { ...RULES, history: 1000 };

import { AVAILABLE, NO_SERVER } from "./support/server";

const member = (id: string, v = 1, s: Json = null): Member => ({ id, v, s });

if (!AVAILABLE) {
  describe("redis adapter", () => {
    it("[RDS-SERVER] a Redis or Valkey server is available", () => {
      throw new Error(NO_SERVER);
    });
  });
} else {
  const redis = useRedis();

  describe("scripts", () => {
    it("[RT-RD-01] computes SHA-1 like the standard, and names every script as the server does", async () => {
      expect(sha1("")).toBe("da39a3ee5e6b4b0d3255bfef95601890afd80709");
      expect(sha1("abc")).toBe("a9993e364706816aba3e25717850c26c9cd0d89d");
      expect(sha1("The quick brown fox jumps over the lazy dog")).toBe(
        "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12",
      );
      expect(sha1("a".repeat(1000))).toBe("291e9a6c66994949b57ba5e650361e98fc36b1ba");
      const raw = redis.raw();
      for (const script of [
        PUBLISH,
        HISTORY,
        KEEPALIVE,
        PRESENCE_SET,
        PRESENCE_REMOVE,
        PRESENCE_TOUCH,
        PRESENCE_REAP,
        PRESENCE_LIST,
      ])
        expect(await raw.sendCommand(["SCRIPT", "LOAD", script.source])).toBe(script.sha);
    });

    it("[RT-RD-02] keeps working after the script cache is flushed, also for commands already in flight", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      expect((await adapter.publish("a", entry(1), RULES)).seq).toBe(1);
      await raw.sendCommand(["SCRIPT", "FLUSH"]);
      expect((await adapter.publish("a", entry(2), RULES)).seq).toBe(2);
      await raw.sendCommand(["SCRIPT", "FLUSH"]);
      const results = await Promise.all(
        Array.from({ length: 30 }, (_, at) => adapter.publish("a", entry(at), RULES)),
      );
      expect(results.map((result) => result.seq).sort((x, y) => x - y)).toEqual(
        Array.from({ length: 30 }, (_, at) => at + 3),
      );
      await raw.sendCommand(["SCRIPT", "FLUSH"]);
      expect((await adapter.history("a", 0, 100)).head).toBe(32);
      await raw.sendCommand(["SCRIPT", "FLUSH"]);
      await adapter.presenceSet("room", member("m1"), 5000);
      await raw.sendCommand(["SCRIPT", "FLUSH"]);
      expect((await adapter.presenceList("room")).map((item) => item.id)).toEqual(["m1"]);
    });
  });

  describe("publish", () => {
    it("[RT-RD-03] forgets an idempotency key when its time is up, in Redis itself", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      const keyed = { ...RULES, key: "once", idempotencyTtlMs: 300 };
      await adapter.publish("a", entry(1), keyed);
      const keys = (await raw.sendCommand(["KEYS", `${redis.prefix()}:{a}:k:*`])) as string[];
      expect(keys).toHaveLength(1);
      const left = (await raw.sendCommand(["PTTL", keys[0] as string])) as number;
      expect(left).toBeGreaterThan(0);
      expect(left).toBeLessThanOrEqual(300);
      await sleep(400);
      expect(await raw.sendCommand(["EXISTS", keys[0] as string])).toBe(0);
      expect(await adapter.publish("a", entry(2), keyed)).toEqual({ seq: 2, duplicate: false });
    });

    it("[RT-RD-04] numbers 20 concurrent publishers on 3 adapters without a gap and delivers in order to every listener", async () => {
      const adapters = [redis.open(), redis.open(), redis.open()];
      const seen = adapters.map(() => [] as number[]);
      await Promise.all(
        adapters.map(({ adapter }, at) =>
          adapter.subscribe("hot", (message) => seen[at]?.push(message.seq)),
        ),
      );
      const per = 25;
      const results: number[] = [];
      await Promise.all(
        Array.from({ length: 20 }, async (_, publisher) => {
          const { adapter } = adapters[publisher % 3] as (typeof adapters)[number];
          for (let at = 0; at < per; at += 1)
            results.push((await adapter.publish("hot", entry(publisher * per + at), RULES)).seq);
        }),
      );
      const total = 20 * per;
      expect(results.sort((x, y) => x - y)).toEqual(
        Array.from({ length: total }, (_, at) => at + 1),
      );
      await until(() => seen.every((list) => list.length === total));
      for (const list of seen)
        expect(list).toEqual(Array.from({ length: total }, (_, at) => at + 1));
    });

    it("[RT-RD-05] keeps exactly the newest `history` messages, and the two indexes agree, under load", async () => {
      const [first, second] = [redis.open(), redis.open()];
      const raw = redis.raw();
      const small = { ...RULES, history: 50 };
      await Promise.all(
        Array.from({ length: 400 }, (_, at) =>
          (at % 2 === 0 ? first : second).adapter.publish("load", entry(at), small),
        ),
      );
      const base = `${redis.prefix()}:{load}`;
      expect(await raw.sendCommand(["ZCARD", `${base}:l`])).toBe(50);
      expect(await raw.sendCommand(["ZCARD", `${base}:t`])).toBe(50);
      const page = await first.adapter.history("load", 0, 1000);
      expect(page.messages.map((message) => message.seq)).toEqual(
        Array.from({ length: 50 }, (_, at) => 351 + at),
      );
      expect(page.head).toBe(400);
      expect(page.oldest).toBe(351);
    });

    it("[RT-RD-06] hides messages older than the history lifetime on read even when no publish trimmed them", async () => {
      const { adapter } = redis.open();
      const short = { ...RULES, historyTtlMs: 150 };
      await adapter.publish("a", entry(1), short);
      await sleep(60);
      await adapter.publish("a", entry(2), short);
      await sleep(120);
      const page = await adapter.history("a", 0, 10);
      expect(page.messages.map((message) => message.seq)).toEqual([2]);
      expect(page.oldest).toBe(2);
      expect(page.head).toBe(2);
      await sleep(200);
      const none = await adapter.history("a", 0, 10);
      expect(none.messages).toEqual([]);
      expect(none.oldest).toBe(0);
      expect(none.head).toBe(2);
    });

    it("[RT-RD-07] restarts the numbering after an idle channel expired, which resuming clients see as a reset", async () => {
      const { adapter } = redis.open({ channelTtlMs: 200 });
      const quick = { ...RULES, historyTtlMs: 100, idempotencyTtlMs: 100 };
      expect((await adapter.publish("a", entry(1), quick)).seq).toBe(1);
      expect((await adapter.publish("a", entry(2), quick)).seq).toBe(2);
      expect((await adapter.history("a", 0, 10)).head).toBe(2);
      await sleep(350);
      expect((await adapter.history("a", 0, 10)).head).toBe(0);
      expect((await adapter.publish("a", entry(3), quick)).seq).toBe(1);
    });

    it("[RT-RD-08] keeps the numbering of a channel alive while a listener is subscribed", async () => {
      const { adapter } = redis.open({ channelTtlMs: 300 });
      const quick = { ...RULES, historyTtlMs: 100, idempotencyTtlMs: 100 };
      const seen: number[] = [];
      await adapter.publish("a", entry(1), quick);
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await sleep(900);
      expect((await adapter.history("a", 0, 10)).head).toBe(1);
      expect((await adapter.publish("a", entry(2), quick)).seq).toBe(2);
      await until(() => seen.length === 1);
      expect(seen).toEqual([2]);
    });

    it("[RT-RD-09] stores and delivers a message at the 64 KiB limit, and awkward text, unchanged", async () => {
      const { adapter } = redis.open();
      const seen: StoredMessage[] = [];
      await adapter.subscribe("big", (message) => seen.push(message));
      const payloads = [
        JSON.stringify("x".repeat(65_536 - 2)),
        JSON.stringify({
          text: 'line\nbreak     \u0000 "quoted" \\ back',
          emoji: "😀🚀",
          arabic: "مرحبا",
        }),
        JSON.stringify('}{"s":9,"a":1,'),
        "null",
      ];
      for (const data of payloads)
        await adapter.publish("big", { event: "e", data, publisher: 'p"1', user: "ü" }, RULES);
      await until(() => seen.length === payloads.length);
      expect(seen.map((message) => message.data)).toEqual(payloads);
      const stored = await adapter.history("big", 0, 10);
      expect(stored.messages.map((message) => message.data)).toEqual(payloads);
      expect(stored.messages[1]?.publisher).toBe('p"1');
      expect(stored.messages[1]?.user).toBe("ü");
      expect(payloads[0]?.length).toBe(65_536);
    });

    it("[RT-RD-10] takes the accepted-at time from Redis, never decreasing within a channel", async () => {
      const { adapter } = redis.open();
      const times: number[] = [];
      for (let at = 0; at < 20; at += 1) await adapter.publish("a", entry(at), RULES);
      for (const message of (await adapter.history("a", 0, 100)).messages) times.push(message.at);
      expect(times.every((time, at) => at === 0 || time >= (times[at - 1] as number))).toBe(true);
      expect(Math.abs((times[0] as number) - Date.now())).toBeLessThan(5000);
    });
  });

  describe("subscriptions", () => {
    it("[RT-RD-11] subscribes a topic once however many listeners, and drops the topic with the last one", async () => {
      const { adapter, client } = redis.open();
      const seenA: number[] = [];
      const seenB: number[] = [];
      const stopA = await adapter.subscribe("a", (message) => seenA.push(message.seq));
      const stopB = await adapter.subscribe("a", (message) => seenB.push(message.seq));
      const subscriber = client.subscribers[0];
      expect(subscriber?.topics).toEqual([`${redis.prefix()}:{a}:c`]);
      await adapter.publish("a", entry(1), RULES);
      await until(() => seenA.length === 1 && seenB.length === 1);
      await stopA();
      expect(subscriber?.topics).toHaveLength(1);
      await adapter.publish("a", entry(2), RULES);
      await until(() => seenB.length === 2);
      expect(seenA).toEqual([1]);
      await stopB();
      await stopB();
      expect(subscriber?.topics).toEqual([]);
      const raw = redis.raw();
      expect(await raw.sendCommand(["PUBSUB", "CHANNELS", `${redis.prefix()}:*`])).toEqual([]);
    });

    it("[RT-RD-12] resubscribes the same topic right after dropping it, and keeps receiving", async () => {
      const { adapter } = redis.open();
      for (let round = 0; round < 20; round += 1) {
        const seen: number[] = [];
        const stop = await adapter.subscribe("a", (message) => seen.push(message.seq));
        await adapter.publish("a", entry(round), RULES);
        await until(() => seen.length === 1);
        void stop();
      }
      const final: number[] = [];
      await adapter.subscribe("a", (message) => final.push(message.seq));
      await adapter.publish("a", entry("end"), RULES);
      await until(() => final.length === 1);
      expect(final).toEqual([21]);
    });

    it("[RT-RD-13] never calls a listener twice for a seq nor with a lower one, even for messages published by anyone", async () => {
      const [first, second] = [redis.open(), redis.open()];
      const seen: number[] = [];
      await first.adapter.subscribe("a", (message) => seen.push(message.seq));
      await Promise.all([
        ...Array.from({ length: 30 }, (_, at) => first.adapter.publish("a", entry(at), RULES)),
        ...Array.from({ length: 30 }, (_, at) => second.adapter.publish("a", entry(at), RULES)),
      ]);
      await until(() => seen.length === 60);
      for (let at = 1; at < seen.length; at += 1)
        expect(seen[at]).toBeGreaterThan(seen[at - 1] as number);
    });

    it("[RT-RD-14] a listener that throws does not stop the others, and is reported", async () => {
      const { adapter, errors } = redis.open();
      const seen: number[] = [];
      await adapter.subscribe("a", () => {
        throw new Error("listener failure");
      });
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await adapter.publish("a", entry(1), RULES);
      await adapter.publish("a", entry(2), RULES);
      await until(() => seen.length === 2);
      expect(errors.map((item) => item.context.where)).toEqual(["listener", "listener"]);
    });
  });

  describe("gaps", () => {
    it("[RT-RD-15] fills a jump in the live sequence from history before delivering the message that jumped", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      const seen: number[] = [];
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await adapter.publish("a", entry(1), RULES);
      await until(() => seen.length === 1);
      // Two messages that were retained but whose live copies this connection never saw.
      const base = `${redis.prefix()}:{a}`;
      const at = String(Date.now());
      for (const seq of [2, 3]) {
        await raw.sendCommand([
          "ZADD",
          `${base}:l`,
          String(seq),
          `{"s":${seq},"a":${at},"e":"e","d":"${seq}"}`,
        ]);
        await raw.sendCommand(["ZADD", `${base}:t`, at, `${String(seq).padStart(16, "0")}`]);
      }
      await raw.sendCommand(["HSET", `${base}:m`, "s", "3"]);
      await adapter.publish("a", entry(4), RULES);
      await until(() => seen.length === 4);
      expect(seen).toEqual([1, 2, 3, 4]);
    });

    it("[RT-RD-16] lets a jump through when history no longer holds the missing messages", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      const seen: number[] = [];
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await adapter.publish("a", entry(1), RULES);
      await until(() => seen.length === 1);
      await raw.sendCommand(["HSET", `${redis.prefix()}:{a}:m`, "s", "9"]);
      await adapter.publish("a", entry(2), RULES);
      await adapter.publish("a", entry(3), RULES);
      await until(() => seen.length === 3);
      expect(seen).toEqual([1, 10, 11]);
    });
  });

  describe("restarted numbering", () => {
    it("[RT-REV-8] delivers the first message after the channel's keys were lost, instead of dropping it as a repeat", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      const seen: { readonly seq: number; readonly data: string }[] = [];
      await adapter.subscribe("a", (message) =>
        seen.push({ seq: message.seq, data: message.data }),
      );
      await adapter.publish("a", entry(1), RULES);
      await adapter.publish("a", entry(2), RULES);
      await until(() => seen.length === 2);
      const base = `${redis.prefix()}:{a}`;
      await raw.sendCommand(["DEL", `${base}:m`, `${base}:l`, `${base}:t`]);
      // The accepted-at time has millisecond resolution: the restarted message must be later than the last one delivered.
      await sleep(5);
      await adapter.publish("a", entry(3), RULES);
      await until(() => seen.length === 3);
      expect(seen[2]).toEqual({ seq: 1, data: "3" });
    });
  });

  describe("reconnect", () => {
    it("[RT-RD-20] recovers every message published while the subscriber connection was down, once and in order", async () => {
      const [reader, writer] = [redis.open(), redis.open()];
      const raw = redis.raw();
      const seen: number[] = [];
      await reader.adapter.subscribe("a", (message) => seen.push(message.seq));
      for (let at = 0; at < 5; at += 1) await writer.adapter.publish("a", entry(at), RULES);
      await until(() => seen.length === 5);
      const subscriber = reader.client.subscribers[0];
      if (subscriber === undefined) throw new Error("no subscriber");
      subscriber.reconnectDelayMs = 400;
      await raw.sendCommand(["CLIENT", "KILL", "ID", String(subscriber.clientId)]);
      for (let at = 5; at < 15; at += 1) await writer.adapter.publish("a", entry(at), RULES);
      await until(() => seen.length === 15, 8000);
      for (let at = 15; at < 20; at += 1) await writer.adapter.publish("a", entry(at), RULES);
      await until(() => seen.length === 20);
      await sleep(100);
      expect(seen).toEqual(Array.from({ length: 20 }, (_, at) => at + 1));
      expect(subscriber.reconnected).toBe(1);
    });

    it("[RT-RD-21] delivers no duplicate when messages arrive live and from the recovery at the same time", async () => {
      const [reader, writer] = [redis.open(), redis.open()];
      const raw = redis.raw();
      const seen: number[] = [];
      await reader.adapter.subscribe("a", (message) => seen.push(message.seq));
      await writer.adapter.publish("a", entry(0), RETAIN);
      await until(() => seen.length === 1);
      const subscriber = reader.client.subscribers[0];
      if (subscriber === undefined) throw new Error("no subscriber");
      subscriber.reconnectDelayMs = 30;
      let next = 1;
      const pump = (async () => {
        while (next < 200) {
          await writer.adapter.publish("a", entry(next), RETAIN);
          next += 1;
        }
      })();
      for (let round = 0; round < 3; round += 1) {
        await raw.sendCommand(["CLIENT", "KILL", "ID", String(subscriber.clientId)]);
        await sleep(80);
      }
      await pump;
      await until(() => seen.length === 200, 8000);
      await sleep(100);
      expect(seen).toEqual(Array.from({ length: 200 }, (_, at) => at + 1));
    });

    it("[RT-RD-22] reconciles presence after a reconnect: one join, set or leave per change that happened meanwhile", async () => {
      const [reader, writer] = [redis.open(), redis.open()];
      const raw = redis.raw();
      const events: string[] = [];
      await writer.adapter.presenceSet("room", member("stays"), 30_000);
      await writer.adapter.presenceSet("room", member("goes"), 30_000);
      await writer.adapter.presenceSet("room", member("changes"), 30_000);
      await reader.adapter.presenceSubscribe("room", (event) =>
        events.push(`${event.op}:${event.member.id}:${event.member.v}`),
      );
      const subscriber = reader.client.subscribers[0];
      if (subscriber === undefined) throw new Error("no subscriber");
      subscriber.reconnectDelayMs = 300;
      await raw.sendCommand(["CLIENT", "KILL", "ID", String(subscriber.clientId)]);
      await writer.adapter.presenceRemove("room", "goes");
      await writer.adapter.presenceSet("room", member("changes", 2, "x"), 30_000);
      await writer.adapter.presenceSet("room", member("arrives"), 30_000);
      await until(() => events.length === 3, 8000);
      await sleep(100);
      expect([...events].sort()).toEqual(["join:arrives:1", "leave:goes:1", "set:changes:2"]);
      await writer.adapter.presenceRemove("room", "arrives");
      await until(() => events.length === 4);
      expect(events[3]).toBe("leave:arrives:1");
    });
  });

  describe("presence", () => {
    it("[RT-RD-30] announces exactly one leave per member when three servers reap and remove at once", async () => {
      const servers = [redis.open(), redis.open(), redis.open()];
      const leaves = servers.map(() => [] as string[]);
      await Promise.all(
        servers.map(({ adapter }, at) =>
          adapter.presenceSubscribe("room", (event) => {
            if (event.op === "leave") leaves[at]?.push(event.member.id);
          }),
        ),
      );
      const ids = Array.from({ length: 40 }, (_, at) => `m${at}`);
      await Promise.all(
        ids.map((id, at) =>
          (servers[at % 3] as (typeof servers)[number]).adapter.presenceSet(
            "room",
            member(id),
            at < 20 ? 80 : 30_000,
          ),
        ),
      );
      await sleep(160);
      const removed = await Promise.all([
        ...servers.map(({ adapter }) => adapter.presenceReap("room")),
        ...ids
          .slice(20, 30)
          .flatMap((id) =>
            servers.map(async ({ adapter }) =>
              (await adapter.presenceRemove("room", id)) ? [id] : [],
            ),
          ),
      ]);
      const gone = removed.flat().map((item) => (typeof item === "string" ? item : item.id));
      expect([...gone].sort()).toEqual([...ids.slice(0, 30)].sort());
      await until(() => leaves.every((list) => list.length === 30));
      await sleep(100);
      for (const list of leaves) expect([...list].sort()).toEqual([...ids.slice(0, 30)].sort());
      expect((await servers[0]?.adapter.presenceList("room"))?.length).toBe(10);
    });

    it("[RT-RD-31] touch refreshes many members in a few calls, across channels, and returns the missing ones", async () => {
      const { adapter } = redis.open();
      const refs: { channel: string; id: string }[] = [];
      for (let at = 0; at < 1200; at += 1) {
        const channel = at % 2 === 0 ? "even" : "odd";
        const id = `m${at}`;
        refs.push({ channel, id });
        if (at % 100 !== 0) await adapter.presenceSet(channel, member(id), 120);
      }
      const missing = await adapter.presenceTouch(refs, 30_000);
      expect(missing.map((ref) => ref.id).sort()).toEqual(
        Array.from({ length: 12 }, (_, at) => `m${at * 100}`).sort(),
      );
      await sleep(250);
      expect(
        (await adapter.presenceList("even")).length + (await adapter.presenceList("odd")).length,
      ).toBe(1188);
    });

    it("[RT-RD-32] lets presence keys expire on their own when nobody reaps, after the grace period", async () => {
      const { adapter } = redis.open({ presenceGraceMs: 150 });
      const raw = redis.raw();
      await adapter.presenceSet("room", member("m1"), 100);
      const base = `${redis.prefix()}:{room}`;
      const left = (await raw.sendCommand(["PTTL", `${base}:p`])) as number;
      expect(left).toBeGreaterThan(0);
      expect(left).toBeLessThanOrEqual(250);
      await sleep(400);
      expect(await raw.sendCommand(["EXISTS", `${base}:p`, `${base}:x`])).toBe(0);
    });

    it("[RT-RD-33] ignores an older version without refreshing, treats an equal one as a silent refresh", async () => {
      const [first, second] = [redis.open(), redis.open()];
      const events: string[] = [];
      await second.adapter.presenceSubscribe("room", (event) =>
        events.push(`${event.op}:${event.member.v}`),
      );
      await first.adapter.presenceSet("room", member("m1", 3, "c"), 30_000);
      await first.adapter.presenceSet("room", member("m1", 2, "b"), 30_000);
      await first.adapter.presenceSet("room", member("m1", 3, "c2"), 30_000);
      await first.adapter.presenceSet("room", member("m1", 4, "d"), 30_000);
      await until(() => events.length === 2);
      await sleep(50);
      expect(events).toEqual(["join:3", "set:4"]);
      expect((await first.adapter.presenceList("room"))[0]).toEqual(member("m1", 4, "d"));
    });

    it("[RT-RD-34] keeps a member's user and any JSON state exactly", async () => {
      const { adapter } = redis.open();
      const state: Json = { a: [1, 2, { b: null }], s: 'x\ny"', n: -0.5, e: [] };
      await adapter.presenceSet("room", { id: "m1", u: "ada", s: state, v: 1 }, 30_000);
      expect(await adapter.presenceList("room")).toEqual([{ id: "m1", u: "ada", s: state, v: 1 }]);
      const events: PresenceEvent[] = [];
      await adapter.presenceSubscribe("room", (event) => events.push(event));
      await adapter.presenceRemove("room", "m1");
      await until(() => events.length === 1);
      expect(events[0]).toEqual({
        channel: "room",
        op: "leave",
        member: { id: "m1", u: "ada", s: state, v: 1 },
      });
    });
  });

  describe("bad data", () => {
    it("[RT-RD-40] skips malformed stored messages and presence entries, reports them, and keeps the rest", async () => {
      const { adapter, errors } = redis.open();
      const raw = redis.raw();
      const base = `${redis.prefix()}:{bad}`;
      await adapter.publish("bad", entry(1), RULES);
      await raw.sendCommand(["ZADD", `${base}:l`, "2", "not json"]);
      await raw.sendCommand(["ZADD", `${base}:l`, "3", '{"s":3,"a":1}']);
      await raw.sendCommand(["ZADD", `${base}:l`, "4", '{"s":4,"a":1,"e":"e","d":5}']);
      await raw.sendCommand(["ZADD", `${base}:l`, "5", "[1,2]"]);
      const soon = String(Date.now() + 60_000);
      for (const padded of [
        "0000000000000002",
        "0000000000000003",
        "0000000000000004",
        "0000000000000005",
      ])
        await raw.sendCommand(["ZADD", `${base}:t`, soon, padded]);
      const page = await adapter.history("bad", 0, 100);
      expect(page.messages.map((message) => message.seq)).toEqual([1]);
      expect(errors.filter((item) => item.context.where === "parse")).toHaveLength(4);

      await adapter.presenceSet("room", member("good"), 30_000);
      const room = `${redis.prefix()}:{room}`;
      await raw.sendCommand(["HSET", `${room}:p`, "broken", "{{{"]);
      await raw.sendCommand(["HSET", `${room}:p`, "shapeless", '{"v":1,"m":{"id":3}}']);
      await raw.sendCommand(["ZADD", `${room}:x`, String(Date.now() + 60_000), "broken"]);
      await raw.sendCommand(["ZADD", `${room}:x`, String(Date.now() + 60_000), "shapeless"]);
      await raw.sendCommand(["ZADD", `${room}:x`, String(Date.now() + 60_000), "orphan"]);
      expect((await adapter.presenceList("room")).map((item) => item.id)).toEqual(["good"]);
      expect(await adapter.presenceRemove("room", "broken")).toBe(true);
      await adapter.presenceSet("room", member("broken", 1), 30_000);
      expect((await adapter.presenceList("room")).map((item) => item.id).sort()).toEqual([
        "broken",
        "good",
      ]);
    });

    it("[RT-RD-41] ignores garbage published on a topic, and a message whose seq is not above the last", async () => {
      const { adapter, errors } = redis.open();
      const raw = redis.raw();
      const seen: number[] = [];
      const events: string[] = [];
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await adapter.presenceSubscribe("a", (event) => events.push(event.op));
      const topic = `${redis.prefix()}:{a}:c`;
      await adapter.publish("a", entry(1), RULES);
      await raw.sendCommand(["PUBLISH", topic, "garbage"]);
      await raw.sendCommand(["PUBLISH", topic, '{"s":0,"a":1,"e":"e","d":"1"}']);
      await raw.sendCommand(["PUBLISH", topic, '{"s":1,"a":1,"e":"e","d":"1"}']);
      await raw.sendCommand(["PUBLISH", `${redis.prefix()}:{a}:e`, "garbage"]);
      await raw.sendCommand(["PUBLISH", `${redis.prefix()}:{a}:e`, '{"o":"nope","m":{}}']);
      await adapter.publish("a", entry(2), RULES);
      await until(() => seen.length === 2);
      await sleep(50);
      expect(seen).toEqual([1, 2]);
      expect(events).toEqual([]);
      expect(errors.filter((item) => item.context.where === "parse")).toHaveLength(4);
    });

    it("[RT-RD-42] reports an error thrown by onError without letting it escape", async () => {
      const client = redis.open().client;
      const adapter = redisAdapter({
        client,
        prefix: redis.prefix(),
        onError: () => {
          throw new Error("reporter failure");
        },
      });
      await adapter.subscribe("a", () => {
        throw new Error("listener failure");
      });
      await adapter.publish("a", entry(1), RULES);
      await sleep(50);
      await adapter.close();
    });
  });

  describe("failure", () => {
    it("[RT-RD-50] rejects with RealtimeError unavailable when the connection is lost, and recovers when it is back", async () => {
      const { adapter, client } = redis.open();
      await adapter.publish("a", entry(1), RULES);
      const pending = Array.from({ length: 5 }, () => adapter.publish("a", entry(2), RULES));
      client.sever();
      const settled = await Promise.allSettled(pending);
      for (const result of settled)
        if (result.status === "rejected") {
          expect(result.reason).toBeInstanceOf(RealtimeError);
          expect((result.reason as RealtimeError).code).toBe("unavailable");
        }
      const after = await adapter.publish("a", entry(3), RULES);
      const head = (await adapter.history("a", 0, 100)).head;
      expect(after.seq).toBe(head);
      const accepted = settled.filter((result) => result.status === "fulfilled").length;
      expect(head).toBeGreaterThanOrEqual(2 + accepted);
    });

    it("[RT-RD-51] maps every transport failure of every method to unavailable, and never half-applies", async () => {
      const failing = redisAdapter({
        client: {
          sendCommand: () => Promise.reject(new Error("ECONNREFUSED")),
          createSubscriber: () => Promise.reject(new Error("ECONNREFUSED")),
        },
        prefix: redis.prefix(),
      });
      const calls: (() => Promise<unknown>)[] = [
        () => failing.publish("a", entry(1), RULES),
        () => failing.history("a", 0, 10),
        () => failing.presenceSet("a", member("m"), 1000),
        () => failing.presenceRemove("a", "m"),
        () => failing.presenceList("a"),
        () => failing.presenceTouch([{ channel: "a", id: "m" }], 1000),
        () => failing.presenceReap("a"),
        () => failing.subscribe("a", () => undefined),
        () => failing.presenceSubscribe("a", () => undefined),
      ];
      for (const call of calls) {
        const error = await call().then(
          () => undefined,
          (reason: unknown) => reason,
        );
        expect(error).toBeInstanceOf(RealtimeError);
        expect((error as RealtimeError).code).toBe("unavailable");
      }
      await failing.close();
    });

    it("[RT-RD-52] fails a subscription cleanly when the subscriber cannot subscribe, and a retry works", async () => {
      const { client } = redis.open();
      let fail = true;
      const flaky = {
        subscribe: async (topic: string, listener: (message: string) => void): Promise<void> => {
          if (fail) throw new Error("subscribe failed");
          await client.createSubscriber().subscribe(topic, listener);
        },
        unsubscribe: async (): Promise<void> => undefined,
        onReconnect: (): (() => void) => () => undefined,
      };
      const adapter = redisAdapter({ client, subscriber: flaky, prefix: redis.prefix() });
      const error = await adapter
        .subscribe("a", () => undefined)
        .then(
          () => undefined,
          (reason: unknown) => reason,
        );
      expect((error as RealtimeError).code).toBe("unavailable");
      fail = false;
      const seen: number[] = [];
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      await adapter.publish("a", entry(1), RULES);
      await until(() => seen.length === 1);
      await adapter.close();
    });

    it("[RT-RD-53] settles every call when the adapter closes while commands are in flight, and refuses afterward", async () => {
      const { adapter, client } = redis.open();
      const seen: number[] = [];
      await adapter.subscribe("a", (message) => seen.push(message.seq));
      const inflight = Array.from({ length: 60 }, (_, at) =>
        adapter.publish("a", entry(at), RULES),
      );
      const closing = adapter.close();
      const settled = await Promise.allSettled(inflight);
      await closing;
      for (const result of settled)
        if (result.status === "rejected")
          expect((result.reason as RealtimeError).code).toBe("unavailable");
      await adapter.close();
      const after = await adapter.publish("a", entry(1), RULES).then(
        () => undefined,
        (reason: unknown) => reason,
      );
      expect((after as RealtimeError).code).toBe("unavailable");
      const late = await adapter
        .subscribe("a", () => undefined)
        .then(
          () => undefined,
          (reason: unknown) => reason,
        );
      expect((late as RealtimeError).code).toBe("unavailable");
      const count = seen.length;
      const other = redis.open();
      await other.adapter.publish("a", entry(2), RULES);
      await sleep(80);
      expect(seen).toHaveLength(count);
      // The caller's client is not the adapter's to close.
      expect(await client.sendCommand(["PING"])).toBe("PONG");
      expect(client.subscribers[0]?.topics).toEqual([]);
    });

    it("[RT-RD-55] releases only what it created: a subscriber passed in stays open, unhooked and usable", async () => {
      const { client } = redis.open();
      const subscriber = client.createSubscriber();
      const adapter = redisAdapter({ client, subscriber, prefix: redis.prefix() });
      await adapter.subscribe("a", () => undefined);
      await adapter.presenceSubscribe("a", () => undefined);
      expect(subscriber.topics).toHaveLength(2);
      expect(subscriber.handlerCount).toBe(2);
      await adapter.close();
      expect(subscriber.topics).toEqual([]);
      expect(subscriber.handlerCount).toBe(0);
      const seen: string[] = [];
      await subscriber.subscribe("mine", (message) => seen.push(message));
      await client.sendCommand(["PUBLISH", "mine", "hello"]);
      await until(() => seen.length === 1);
      await subscriber.close();
    });

    it("[RT-RD-54] leaves nothing half-applied when a publish is refused for its arguments", async () => {
      const { adapter } = redis.open();
      await expect(adapter.publish("a", entry(1), { ...RULES, historyTtlMs: 0 })).rejects.toThrow(
        RangeError,
      );
      await expect(adapter.publish("a{b}", entry(1), RULES)).rejects.toMatchObject({
        code: "bad_channel",
      });
      await expect(adapter.publish("a", entry(1), { ...RULES, key: "" })).rejects.toMatchObject({
        code: "bad_frame",
      });
      await expect(
        adapter.publish("a", { event: "e", data: 5 as unknown as string }, RULES),
      ).rejects.toThrow(TypeError);
      expect((await adapter.history("a", 0, 10)).head).toBe(0);
    });
  });

  describe("options and layout", () => {
    it("[RT-RD-60] validates the options", () => {
      const client = redis.open().client;
      expect(() => redisAdapter({ client, prefix: "a{b}" })).toThrow(RangeError);
      expect(() => redisAdapter({ client, prefix: "" })).toThrow(RangeError);
      expect(() => redisAdapter({ client, channelTtlMs: 0 })).toThrow(RangeError);
      expect(() => redisAdapter({ client, presenceGraceMs: -1 })).toThrow(RangeError);
      expect(() =>
        redisAdapter({ client: { sendCommand: client.sendCommand.bind(client) } }),
      ).toThrow(TypeError);
      expect(() => redisAdapter({ client: {} as never })).toThrow(TypeError);
    });

    it("[RT-RD-61] keeps all keys of a channel in one Redis Cluster hash slot", async () => {
      const { adapter } = redis.open();
      const raw = redis.raw();
      const channel = "chat:room/1@x";
      await adapter.publish(channel, entry(1), { ...RULES, key: "k1" });
      await adapter.presenceSet(channel, member("m1"), 30_000);
      await adapter.subscribe(channel, () => undefined);
      const keys = (await raw.sendCommand(["KEYS", `${redis.prefix()}:*`])) as string[];
      expect(keys.length).toBeGreaterThanOrEqual(6);
      expect(new Set(keys.map(slotOf)).size).toBe(1);
      const topics = (await raw.sendCommand([
        "PUBSUB",
        "CHANNELS",
        `${redis.prefix()}:*`,
      ])) as string[];
      expect(topics).toHaveLength(1);
      expect(new Set([...keys, ...topics].map(slotOf)).size).toBe(1);
      for (const key of keys) expect(key).toContain(`{${channel}}`);
    });

    it("[RT-RD-62] separates prefixes completely", async () => {
      const one = redis.open({ prefix: `${redis.prefix()}-one` });
      const two = redis.open({ prefix: `${redis.prefix()}-two` });
      const seen: number[] = [];
      await two.adapter.subscribe("a", (message) => seen.push(message.seq));
      await one.adapter.publish("a", entry(1), RULES);
      await one.adapter.presenceSet("a", member("m1"), 30_000);
      await sleep(60);
      expect(seen).toEqual([]);
      expect(await two.adapter.presenceList("a")).toEqual([]);
      expect((await two.adapter.history("a", 0, 10)).head).toBe(0);
    });
  });

  describe("throughput", () => {
    it("[RT-RD-70] publishes a pipelined burst and delivers it in order", async () => {
      const [writer, reader] = [redis.open(), redis.open()];
      const total = 2000;
      const seen: number[] = [];
      await reader.adapter.subscribe("burst", (message) => seen.push(message.seq));
      const started = performance.now();
      await Promise.all(
        Array.from({ length: total }, (_, at) => writer.adapter.publish("burst", entry(at), RULES)),
      );
      const publishMs = performance.now() - started;
      await until(() => seen.length === total, 10_000);
      const deliverMs = performance.now() - started;
      expect(seen).toEqual(Array.from({ length: total }, (_, at) => at + 1));
      if (process.env["REDIS_BENCH"] !== undefined)
        process.stdout.write(
          `redis publish: ${Math.round((total / publishMs) * 1000)} /s pipelined, delivered ${Math.round((total / deliverMs) * 1000)} /s\n`,
        );
    });
  });
}

/** CRC16 (XMODEM) as Redis Cluster uses it to pick a slot, honoring a `{hash tag}`. */
function slotOf(key: string): number {
  const open = key.indexOf("{");
  const close = open === -1 ? -1 : key.indexOf("}", open + 1);
  const tag = open !== -1 && close > open + 1 ? key.slice(open + 1, close) : key;
  let crc = 0;
  for (const byte of new TextEncoder().encode(tag)) {
    crc ^= byte << 8;
    for (let bit = 0; bit < 8; bit += 1)
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  }
  return crc % 16384;
}

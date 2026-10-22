import { afterEach, describe, expect, it } from "vitest";
import type { Adapter } from "../src/adapter";
import type { ServerFrame } from "../src/index";
import { type RedisClient, redisAdapter } from "../src/redis";
import { createHub, type Hub } from "../src/server/index";
import { connectTest, type TestConnection } from "../src/testing";
import { sleep, until, useRedis } from "./support/fixture";
import { AVAILABLE, NO_SERVER } from "./support/server";

type Msg = Extract<ServerFrame, { t: "msg" }>;
type Pres = Extract<ServerFrame, { t: "pres" }>;

const msgs = (connection: TestConnection): Msg[] =>
  connection.frames.filter((frame): frame is Msg => frame.t === "msg");
const pres = (connection: TestConnection): Pres[] =>
  connection.frames.filter((frame): frame is Pres => frame.t === "pres");

if (!AVAILABLE) {
  describe("redis adapter with the hub", () => {
    it("[RDS-SERVER] a Redis or Valkey server is available", () => {
      throw new Error(NO_SERVER);
    });
  });
} else {
  const redis = useRedis();
  const hubs: Hub[] = [];
  const adapters: Adapter[] = [];
  const errors: unknown[] = [];

  /** A hub over its own adapter (its own connections) on the test's prefix, as a second server would have. */
  function server(options: { readonly client?: (client: RedisClient) => RedisClient } = {}): {
    readonly hub: Hub;
    readonly adapter: Adapter;
    readonly opened: ReturnType<typeof redis.open>;
  } {
    const opened = redis.open();
    const wrapped = options.client?.(opened.client);
    const adapter =
      wrapped === undefined
        ? opened.adapter
        : redisAdapter({
            client: wrapped,
            prefix: redis.prefix(),
            subscriber: opened.client.createSubscriber(),
          });
    adapters.push(adapter);
    const hub = createHub({
      adapter,
      channels: { "chat:{room}": { subscribe: true, publish: true, presence: true, history: 200 } },
      limits: {
        presenceTtlMs: 600,
        presenceHeartbeatMs: 100,
        publishesPerSecond: 10_000,
        publishBurst: 10_000,
        channelPublishesPerSecond: 10_000,
      },
      onError: (error) => errors.push(error),
    });
    hubs.push(hub);
    return { hub, adapter, opened };
  }

  afterEach(async () => {
    await Promise.all(hubs.splice(0).map((hub) => hub.close().catch(() => undefined)));
    await Promise.all(adapters.splice(0).map((adapter) => adapter.close().catch(() => undefined)));
    errors.length = 0;
  });

  describe("two hubs on one redis adapter", () => {
    it("[RT-RD-90] delivers a publish on hub 1 to a subscriber on hub 2, in order, with the ack on the publisher", async () => {
      const one = server();
      const two = server();
      const subscriber = connectTest(two.hub);
      const publisher = connectTest(one.hub);
      subscriber.send({ t: "sub", n: 1, c: "chat:lobby" });
      await subscriber.settled();
      publisher.send({ t: "sub", n: 1, c: "chat:lobby", echo: false });
      for (let at = 0; at < 20; at += 1)
        publisher.send({ t: "pub", c: "chat:lobby", e: "say", d: { at }, n: at + 2 });
      await publisher.settled();
      await until(() => msgs(subscriber).length === 20);
      expect(msgs(subscriber).map((frame) => frame.s)).toEqual(
        Array.from({ length: 20 }, (_, at) => at + 1),
      );
      expect(msgs(subscriber).map((frame) => frame.d)).toEqual(
        Array.from({ length: 20 }, (_, at) => ({ at })),
      );
      const acks = publisher.frames.filter((frame) => frame.t === "ack");
      expect(acks.map((frame) => (frame.t === "ack" ? frame.s : 0))).toEqual(
        Array.from({ length: 20 }, (_, at) => at + 1),
      );
      expect(errors).toEqual([]);
    });

    it("[RT-RD-91] resumes on hub 2 after hub 1 is gone: nothing retained is lost or repeated", async () => {
      const one = server();
      const two = server();
      const client = connectTest(one.hub);
      client.send({ t: "sub", n: 1, c: "chat:lobby" });
      await client.settled();
      const writer = connectTest(two.hub);
      for (let at = 0; at < 3; at += 1)
        writer.send({ t: "pub", c: "chat:lobby", e: "say", d: at, n: at + 1 });
      await writer.settled();
      await until(() => msgs(client).length === 3);
      const last = msgs(client).at(-1)?.s ?? 0;
      await one.hub.close();
      await one.adapter.close();
      for (let at = 3; at < 8; at += 1)
        writer.send({ t: "pub", c: "chat:lobby", e: "say", d: at, n: at + 1 });
      await writer.settled();

      const resumed = connectTest(two.hub);
      resumed.send({ t: "sub", n: 1, c: "chat:lobby", after: last });
      await resumed.settled();
      writer.send({ t: "pub", c: "chat:lobby", e: "say", d: 8, n: 100 });
      await writer.settled();
      await until(() => msgs(resumed).length === 6);
      expect(msgs(resumed).map((frame) => frame.s)).toEqual([4, 5, 6, 7, 8, 9]);
      expect(msgs(resumed).map((frame) => frame.r)).toEqual([1, 1, 1, 1, 1, undefined]);
      const subbed = resumed.frames.find((frame) => frame.t === "subbed");
      expect(subbed?.t === "subbed" && subbed.gap).toBeFalsy();
    });

    it("[RT-RD-92] shows presence across hubs: sync, set, and exactly one leave", async () => {
      const one = server();
      const two = server();
      const member = connectTest(one.hub, { user: "ada" });
      member.send({ t: "sub", n: 1, c: "chat:lobby", presence: { typing: false } });
      await member.settled();
      const watcher = connectTest(two.hub);
      watcher.send({ t: "sub", n: 1, c: "chat:lobby", watch: true });
      await watcher.settled();
      const sync = pres(watcher).find((frame) => frame.op === "sync");
      expect(sync?.m).toEqual([{ id: member.id, u: "ada", s: { typing: false }, v: 1 }]);
      member.send({ t: "pres", c: "chat:lobby", s: { typing: true } });
      await member.settled();
      await until(() => pres(watcher).some((frame) => frame.op === "set"));
      await member.close();
      await until(() => pres(watcher).some((frame) => frame.op === "leave"));
      await sleep(150);
      expect(pres(watcher).map((frame) => frame.op)).toEqual(["sync", "set", "leave"]);
    });

    it("[RT-RD-93] reaps the members of a crashed server, announcing one leave to the survivors", async () => {
      let broken = false;
      const one = server({
        client: (client) => ({
          sendCommand: (args) =>
            broken ? Promise.reject(new Error("ECONNRESET")) : client.sendCommand(args),
        }),
      });
      const two = server();
      const member = connectTest(one.hub);
      member.send({ t: "sub", n: 1, c: "chat:lobby", presence: "hi" });
      await member.settled();
      const watcherA = connectTest(two.hub);
      watcherA.send({ t: "sub", n: 1, c: "chat:lobby", watch: true });
      await watcherA.settled();
      expect(pres(watcherA).find((frame) => frame.op === "sync")?.m).toHaveLength(1);
      broken = true;
      await until(() => pres(watcherA).some((frame) => frame.op === "leave"), 8000);
      await sleep(300);
      expect(pres(watcherA).filter((frame) => frame.op === "leave")).toHaveLength(1);
      const later = connectTest(two.hub);
      later.send({ t: "sub", n: 1, c: "chat:lobby", watch: true });
      await later.settled();
      expect(pres(later).find((frame) => frame.op === "sync")?.m).toEqual([]);
    });

    it("[RT-RD-94] keeps a subscriber on hub 2 gapless and duplicate-free while its pub/sub connection is killed", async () => {
      const one = server();
      const two = server();
      const raw = redis.raw();
      const subscriber = connectTest(two.hub);
      subscriber.send({ t: "sub", n: 1, c: "chat:lobby" });
      await subscriber.settled();
      const connection = two.opened.client.subscribers[0];
      if (connection === undefined) throw new Error("no subscriber connection");
      connection.reconnectDelayMs = 150;
      const writer = connectTest(one.hub);
      writer.send({ t: "sub", n: 1, c: "chat:lobby", echo: false });
      const total = 150;
      let sent = 0;
      const publishUpTo = (count: number): void => {
        for (; sent < count; sent += 1)
          writer.send({ t: "pub", c: "chat:lobby", e: "say", d: sent, n: sent + 2 });
      };
      publishUpTo(40);
      await writer.settled();
      for (const round of [1, 2]) {
        await raw.sendCommand(["CLIENT", "KILL", "ID", String(connection.clientId)]);
        publishUpTo(40 + round * 40);
        await writer.settled();
        await until(() => connection.reconnected >= round, 8000);
      }
      publishUpTo(total);
      await writer.settled();
      await until(() => msgs(subscriber).length === total, 10_000);
      await sleep(100);
      expect(msgs(subscriber).map((frame) => frame.s)).toEqual(
        Array.from({ length: total }, (_, at) => at + 1),
      );
      expect(connection.reconnected).toBeGreaterThanOrEqual(2);
    });
  });
}

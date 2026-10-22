import { afterEach, describe, expect, it } from "vitest";
import { type Adapter, createHub, type Hub, memoryAdapter } from "../src/server/index";
import { connectTest } from "../src/testing";

const open: Hub[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((hub) => hub.close()));
});
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const rules = { "p:{x}": { subscribe: true, presence: true } };
const quiet = (): void => undefined;

describe("presence lifetime", () => {
  it("[RT-G11] a connected member survives many TTLs because the hub heartbeats for it", async () => {
    const adapter = memoryAdapter();
    const hub = createHub({
      adapter,
      channels: rules,
      limits: { presenceTtlMs: 120, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    open.push(hub);
    const a = connectTest(hub, { user: "ann" });
    a.send({ t: "sub", n: 1, c: "p:1", presence: null });
    await a.settled();
    await sleep(500);
    expect(await adapter.presenceList("p:1")).toHaveLength(1);
  });

  it("[RT-G11] a member whose server died is removed after the TTL, announced once, to watchers on another server", async () => {
    const adapter = memoryAdapter();
    // The first server "crashes": it neither heartbeats nor removes its members. Its adapter view ignores those calls.
    const crashing: Adapter = {
      ...adapter,
      presenceRemove: async () => false,
      presenceTouch: async () => [],
    };
    const dying = createHub({
      adapter: crashing,
      channels: rules,
      limits: { presenceTtlMs: 100, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    const survivor = createHub({
      adapter,
      channels: rules,
      limits: { presenceTtlMs: 100, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    open.push(survivor);
    const watcher = connectTest(survivor);
    watcher.send({ t: "sub", n: 1, c: "p:1", watch: true });
    await watcher.settled();
    const victim = connectTest(dying, { user: "ann" });
    victim.send({ t: "sub", n: 1, c: "p:1", presence: null });
    await victim.settled();
    await victim.close();
    await sleep(400);
    const ops = watcher.frames.flatMap((frame) => (frame.t === "pres" ? [frame.op] : []));
    expect(ops).toEqual(["sync", "join", "leave"]);
    expect(await adapter.presenceList("p:1")).toHaveLength(0);
    await dying.close();
  });

  it("[RT-G11] a connection that closes while its join is still being written leaves no member behind", async () => {
    const base = memoryAdapter();
    const slow: Adapter = {
      ...base,
      presenceSet: async (...args) => {
        await sleep(40);
        return base.presenceSet(...args);
      },
    };
    const hub = createHub({ adapter: slow, channels: rules, onError: quiet });
    open.push(hub);
    const a = connectTest(hub, { user: "ann" });
    a.send({ t: "sub", n: 1, c: "p:1", presence: null });
    await sleep(10);
    await a.close();
    await sleep(120);
    expect(await base.presenceList("p:1")).toHaveLength(0);
  });

  it("[RT-G11] a member whose entry expired during an outage is set again by the next heartbeat", async () => {
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
      channels: rules,
      limits: { presenceTtlMs: 90, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    open.push(hub);
    const a = connectTest(hub, { user: "ann" });
    a.send({ t: "sub", n: 1, c: "p:1", presence: { here: true } });
    await a.settled();
    outage = true;
    await sleep(250);
    await base.presenceReap("p:1");
    expect(await base.presenceList("p:1")).toHaveLength(0);
    outage = false;
    await sleep(120);
    expect((await base.presenceList("p:1"))[0]?.id).toBe(a.id);
  });
});

describe("a consumer that cannot catch up", () => {
  it("[RT-G9] is resubscribed from its last seq (unsubbed gap) instead of buffering without bound", async () => {
    const base = memoryAdapter();
    const slow: Adapter = {
      ...base,
      history: async (...args) => {
        await sleep(200);
        return base.history(...args);
      },
    };
    const hub = createHub({
      adapter: slow,
      channels: { "q:{x}": { subscribe: true, publish: true, history: 100 } },
      onError: quiet,
    });
    open.push(hub);
    for (let at = 1; at <= 50; at += 1) await hub.publish("q:1", "m", at);
    const a = connectTest(hub);
    a.send({ t: "sub", n: 1, c: "q:1", after: 0 });
    await sleep(20);
    for (let at = 51; at <= 10_100; at += 1) await hub.publish("q:1", "m", at);
    await sleep(500);
    expect(a.frames.find((frame) => frame.t === "unsubbed")).toEqual({
      t: "unsubbed",
      c: "q:1",
      why: "gap",
    });
  }, 20_000);
});

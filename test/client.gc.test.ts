import { describe, expect, it } from "vitest";
import {
  type Channel,
  type ChannelStatus,
  createRealtime,
  type Realtime,
  type Status,
} from "../src/client/index";
import { createHub } from "../src/server/index";
import { fetchFactory } from "./fixtures/memory-fetch";
import { socketFactory } from "./fixtures/memory-socket";

function exposedGc(): () => void {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (gc === undefined) throw new Error("the gc project must run with --expose-gc");
  return gc;
}

async function collect(): Promise<void> {
  const gc = exposedGc();
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  gc();
  gc();
}

/** Resolves when `register`'s source reports a value `matches` accepts (or at once if `now` already does). */
function reach<T>(
  now: T,
  matches: (value: T) => boolean,
  register: (callback: (value: T) => void) => () => void,
): Promise<void> {
  if (matches(now)) return Promise.resolve();
  return new Promise((resolve) => {
    const stop = register((value) => {
      if (!matches(value)) return;
      stop();
      resolve();
    });
  });
}
const channelIs = (channel: Channel, status: ChannelStatus): Promise<void> =>
  reach(
    channel.status,
    (value) => value === status,
    (callback) => channel.onStatus(callback),
  );
const clientIs = (rt: Realtime, status: Status): Promise<void> =>
  reach(
    rt.status,
    (value) => value === status,
    (callback) => rt.onStatus(callback),
  );

/** Counts the `online` listeners the client adds to the global scope. */
function countingGlobal(): { readonly listeners: () => number; readonly restore: () => void } {
  const scope = globalThis as Record<string, unknown>;
  const table = new Set<unknown>();
  const saved = { add: scope["addEventListener"], remove: scope["removeEventListener"] };
  scope["addEventListener"] = (_type: string, listener: unknown) => table.add(listener);
  scope["removeEventListener"] = (_type: string, listener: unknown) => table.delete(listener);
  return {
    listeners: () => table.size,
    restore() {
      scope["addEventListener"] = saved.add;
      scope["removeEventListener"] = saved.remove;
    },
  };
}

const activeTimers = (): number =>
  process.getActiveResourcesInfo().filter((name) => name === "Timeout").length;

describe("the client does not leak", () => {
  it("[RT-C-54] 5,000 subscribe, reconnect and unsubscribe cycles leave no timer, no listener, no subscription and a flat heap", async () => {
    const counting = countingGlobal();
    const hub = createHub({
      channels: { "u:{id}": { subscribe: true, publish: true, presence: true } },
      limits: { publishBurst: 1000, publishesPerSecond: 1000 },
      onError: () => undefined,
    });
    const sockets = socketFactory(() => hub);
    const fetches = fetchFactory(() => hub);
    const timersBefore = activeTimers();
    try {
      const rt = createRealtime({
        url: "http://localhost/rt",
        transport: "ws",
        WebSocket: sockets.Socket,
        fetch: fetches.fetch,
        reconnect: { random: () => 0 },
        onError: () => undefined,
      });
      const cycle = async (index: number): Promise<void> => {
        const channel = rt.channel(`u:${index % 50}`);
        const stop = channel.on("m", () => undefined);
        await channelIs(channel, "live");
        void channel.publish("m", index, { ack: false });
        sockets.live()[0]?.drop();
        await clientIs(rt, "reconnecting");
        await clientIs(rt, "open");
        await channelIs(channel, "live");
        stop();
        await channelIs(channel, "idle");
      };
      for (let index = 0; index < 500; index += 1) await cycle(index);
      sockets.prune();
      await collect();
      const before = process.memoryUsage().heapUsed;
      for (let index = 0; index < 5000; index += 1) {
        await cycle(index);
        if (index % 500 === 0) sockets.prune();
      }
      expect(counting.listeners()).toBe(1);
      expect(rt.status).toBe("open");
      rt.close();
      sockets.prune();
      await collect();
      await collect();
      const growth = process.memoryUsage().heapUsed - before;
      expect(counting.listeners()).toBe(0);
      expect(hub.stats()).toMatchObject({ connections: 0, channels: 0, subscriptions: 0 });
      expect(activeTimers()).toBeLessThanOrEqual(timersBefore);
      expect(growth).toBeLessThan(6 * 1024 * 1024);
    } finally {
      counting.restore();
      await hub.close();
    }
  }, 180_000);

  it("[RT-C-55] closed clients and closed channels are collected", async () => {
    const hub = createHub({
      channels: { "u:{id}": { subscribe: true, presence: true } },
      onError: () => undefined,
    });
    const sockets = socketFactory(() => hub);
    const refs: WeakRef<object>[] = [];
    try {
      for (let index = 0; index < 200; index += 1) {
        const rt = createRealtime({
          url: "http://localhost/rt",
          transport: "ws",
          WebSocket: sockets.Socket,
          onError: () => undefined,
        });
        const channel = rt.channel(`u:${index}`, { presence: index });
        channel.on("m", () => undefined);
        await channelIs(channel, "live");
        refs.push(new WeakRef(rt), new WeakRef(channel));
        rt.close();
      }
      sockets.prune();
      await collect();
      await collect();
      const alive = refs.filter((ref) => ref.deref() !== undefined).length;
      expect(alive).toBeLessThan(10);
    } finally {
      await hub.close();
    }
  }, 60_000);
});

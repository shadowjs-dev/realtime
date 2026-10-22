import { describe, expect, it } from "vitest";
import { createHub } from "../src/server/index";
import { connectTest } from "../src/testing";

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

describe("the hub does not leak", () => {
  it("[RT-SCALE-01] 20,000 connect, subscribe, publish and close cycles leave no state and a flat heap", async () => {
    const hub = createHub({
      channels: {
        "u:{id}": {
          subscribe: true,
          publish: true,
          presence: true,
          handlers: { echo: ({ data }) => data },
        },
      },
      limits: { publishBurst: 1000, publishesPerSecond: 1000 },
      onError: () => undefined,
    });
    const cycle = async (index: number): Promise<void> => {
      const connection = connectTest(hub, { user: `user${index}` });
      const channel = `u:${index % 500}`;
      connection.send({ t: "sub", n: 1, c: channel, presence: { at: index } });
      connection.send({ t: "pub", c: channel, e: "m", d: index, n: 2 });
      connection.send({ t: "req", n: 3, c: channel, e: "echo", d: index });
      await connection.settled();
      await connection.close();
    };
    for (let index = 0; index < 2000; index += 1) await cycle(index);
    await collect();
    const before = process.memoryUsage().heapUsed;
    for (let index = 0; index < 20_000; index += 1) await cycle(index);
    await collect();
    await collect();
    const growth = process.memoryUsage().heapUsed - before;
    expect(hub.stats()).toMatchObject({ connections: 0, channels: 0, subscriptions: 0 });
    // The retained history of 500 channels (100 messages each at most) is the only legitimate growth.
    expect(growth).toBeLessThan(8 * 1024 * 1024);
    await hub.close();
  }, 120_000);

  it("[RT-SCALE-01] closed connections are collected", async () => {
    const hub = createHub({
      channels: { "u:{id}": { subscribe: true } },
      onError: () => undefined,
    });
    const refs: WeakRef<object>[] = [];
    for (let index = 0; index < 200; index += 1) {
      const connection = connectTest(hub);
      connection.send({ t: "sub", n: 1, c: `u:${index}` });
      await connection.settled();
      refs.push(new WeakRef(connection.frames));
      await connection.close();
    }
    await collect();
    await collect();
    const alive = refs.filter((ref) => ref.deref() !== undefined).length;
    expect(alive).toBeLessThan(5);
    await hub.close();
  });
});

import fc from "fast-check";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Adapter, PublishOptions } from "../src/adapter";
import { cloudflareAdapter } from "../src/cloudflare";
import { memoryAdapter } from "../src/memory";
import type { Member } from "../src/protocol";
import { FakeNamespace } from "./support/do-host";

// The in-memory adapter is the reference of the adapter rules in the realtime protocol reference. The same random sequence of operations goes
// to it and to the Durable Object adapter (channel object, storage double, no sockets involved); every answer must be equal.

type Operation =
  | {
      readonly op: "publish";
      readonly channel: string;
      readonly data: number;
      readonly rules: PublishOptions;
    }
  | {
      readonly op: "history";
      readonly channel: string;
      readonly after: number;
      readonly limit: number;
    }
  | { readonly op: "advance"; readonly ms: number }
  | {
      readonly op: "set";
      readonly channel: string;
      readonly id: string;
      readonly v: number;
      readonly ttlMs: number;
    }
  | { readonly op: "remove"; readonly channel: string; readonly id: string }
  | {
      readonly op: "touch";
      readonly refs: readonly { readonly channel: string; readonly id: string }[];
      readonly ttlMs: number;
    }
  | { readonly op: "list"; readonly channel: string }
  | { readonly op: "reap"; readonly channel: string };

const channel = fc.constantFrom("a", "b");
const id = fc.constantFrom("m1", "m2", "m3");
const rules: fc.Arbitrary<PublishOptions> = fc.record(
  {
    history: fc.constantFrom(0, 1, 3, 10, 100),
    // Retention by time is checked by the unit tests: the reference only trims at publish, the object also on read.
    historyTtlMs: fc.constant(10_000_000),
    key: fc.constantFrom("k1", "k2", "k3"),
    idempotencyTtlMs: fc.constantFrom(30, 500, 100_000),
  },
  { requiredKeys: ["history", "historyTtlMs", "idempotencyTtlMs"] },
);
const operation: fc.Arbitrary<Operation> = fc.oneof(
  {
    weight: 5,
    arbitrary: fc.record({
      op: fc.constant("publish" as const),
      channel,
      data: fc.nat(1000),
      rules,
    }),
  },
  {
    weight: 3,
    arbitrary: fc.record({
      op: fc.constant("history" as const),
      channel,
      after: fc.nat(30),
      limit: fc.constantFrom(0, 1, 2, 5, 50),
    }),
  },
  {
    weight: 3,
    arbitrary: fc.record({
      op: fc.constant("advance" as const),
      ms: fc.constantFrom(1, 25, 50, 400, 1000),
    }),
  },
  {
    weight: 3,
    arbitrary: fc.record({
      op: fc.constant("set" as const),
      channel,
      id,
      v: fc.integer({ min: 1, max: 4 }),
      ttlMs: fc.constantFrom(50, 400, 100_000),
    }),
  },
  { weight: 2, arbitrary: fc.record({ op: fc.constant("remove" as const), channel, id }) },
  {
    weight: 2,
    arbitrary: fc.record({
      op: fc.constant("touch" as const),
      refs: fc.array(fc.record({ channel, id }), { maxLength: 4 }),
      ttlMs: fc.constantFrom(50, 400, 100_000),
    }),
  },
  { weight: 2, arbitrary: fc.record({ op: fc.constant("list" as const), channel }) },
  { weight: 2, arbitrary: fc.record({ op: fc.constant("reap" as const), channel }) },
);

async function apply(adapter: Adapter, step: Operation): Promise<unknown> {
  const byId = (members: readonly Member[]): Member[] =>
    [...members].sort((x, y) => x.id.localeCompare(y.id));
  switch (step.op) {
    case "publish":
      return adapter.publish(step.channel, { event: "e", data: String(step.data) }, step.rules);
    case "history":
      return adapter.history(step.channel, step.after, step.limit);
    case "advance":
      vi.advanceTimersByTime(step.ms);
      return null;
    case "set":
      return adapter.presenceSet(step.channel, { id: step.id, v: step.v, s: step.v }, step.ttlMs);
    case "remove":
      return adapter.presenceRemove(step.channel, step.id);
    case "touch":
      return [...(await adapter.presenceTouch(step.refs, step.ttlMs))]
        .map((ref) => `${ref.channel}/${ref.id}`)
        .sort();
    case "list":
      return byId(await adapter.presenceList(step.channel));
    case "reap":
      return byId(await adapter.presenceReap(step.channel));
  }
}

let space: FakeNamespace | undefined;
beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"], now: 5_000_000 });
});
afterEach(() => {
  space?.dispose();
  space = undefined;
  vi.useRealTimers();
});

describe("cloudflare adapter against the in-memory reference", () => {
  it("[RT-CF-150] answers every operation of a random sequence exactly as the reference does", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(operation, { minLength: 20, maxLength: 80, size: "max" }),
        async (steps) => {
          vi.setSystemTime(5_000_000);
          const reference = memoryAdapter();
          space = new FakeNamespace();
          const subject = cloudflareAdapter({
            namespace: space,
            keepAliveMs: 0,
            onError: () => undefined,
          });
          try {
            for (const step of steps) {
              const expected = await apply(reference, step);
              const actual = await apply(subject, step);
              expect(actual, JSON.stringify(step)).toEqual(expected);
            }
          } finally {
            await subject.close();
            await reference.close();
            space.dispose();
          }
        },
      ),
      { numRuns: 400 },
    );
  });
});

import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { applyPresence } from "../src/client/presence";
import type { Member } from "../src/index";
import { rig } from "./fixtures/harness";

type Command =
  | { readonly k: "pub"; readonly n: number }
  | { readonly k: "drop" }
  | { readonly k: "ghost" }
  | { readonly k: "peer" }
  | { readonly k: "refuse"; readonly n: number }
  | { readonly k: "wait"; readonly ms: number };

const command: fc.Arbitrary<Command> = fc.oneof(
  {
    weight: 5,
    arbitrary: fc.record({ k: fc.constant("pub" as const), n: fc.integer({ min: 1, max: 3 }) }),
  },
  { weight: 4, arbitrary: fc.record({ k: fc.constant("drop" as const) }) },
  { weight: 2, arbitrary: fc.record({ k: fc.constant("ghost" as const) }) },
  { weight: 2, arbitrary: fc.record({ k: fc.constant("peer" as const) }) },
  {
    weight: 1,
    arbitrary: fc.record({ k: fc.constant("refuse" as const), n: fc.integer({ min: 1, max: 3 }) }),
  },
  {
    weight: 3,
    arbitrary: fc.record({ k: fc.constant("wait" as const), ms: fc.integer({ min: 0, max: 3 }) }),
  },
);

describe("random interleavings of publish, drop and reconnect", () => {
  it("[RT-C-53] never lose, duplicate or reorder a message inside retention, and every acknowledgment is the delivered sequence", async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(command, { minLength: 1, maxLength: 24 }), async (commands) => {
        const r = rig(
          { reconnect: { random: () => 0 }, maxQueued: 1000 },
          {
            limits: {
              publishBurst: 1000,
              publishesPerSecond: 1000,
              channelPublishesPerSecond: 1000,
            },
          },
        );
        try {
          const lobby = r.rt.channel("chat:lobby");
          const arrivals: { readonly data: unknown; readonly seq: number }[] = [];
          lobby.on("message", (data, meta) => arrivals.push({ data, seq: meta.seq }));
          await r.until(() => expect(lobby.status).toBe("live"));
          const peer = r.peer();
          const acks: Promise<{ readonly seq: number; readonly duplicate: boolean }>[] = [];
          const sent: string[] = [];
          let counter = 0;
          for (const step of commands) {
            switch (step.k) {
              case "pub":
                for (let at = 0; at < step.n; at += 1) {
                  counter += 1;
                  const value = `c${counter}`;
                  sent.push(value);
                  acks.push(lobby.publish("message", value));
                }
                break;
              case "ghost": {
                const socket = r.sockets.live()[0];
                socket?.stallInbound();
                counter += 1;
                const value = `c${counter}`;
                sent.push(value);
                acks.push(lobby.publish("message", value));
                await new Promise((resolve) => setTimeout(resolve, 1));
                socket?.drop();
                break;
              }
              case "drop":
                r.sockets.live()[0]?.drop();
                break;
              case "refuse":
                r.sockets.refuseNext(step.n);
                r.sockets.live()[0]?.drop();
                break;
              case "peer": {
                counter += 1;
                const value = `p${counter}`;
                sent.push(value);
                peer.send({ t: "pub", c: "chat:lobby", e: "message", d: value });
                break;
              }
              case "wait":
                await new Promise((resolve) => setTimeout(resolve, step.ms));
                break;
            }
          }
          r.sockets.allow();
          const results = await Promise.all(acks);
          await r.until(() => expect(arrivals.length).toBe(sent.length), 5000);
          // Gapless and strictly increasing: no loss, no duplicate, no reorder.
          expect(arrivals.map((item) => item.seq)).toEqual(
            arrivals.map((_item, index) => index + 1),
          );
          // Everything sent arrived exactly once.
          expect([...arrivals.map((item) => item.data)].sort()).toEqual([...sent].sort());
          // This client's own publishes keep the order they were made in.
          const own = arrivals
            .map((item) => String(item.data))
            .filter((value) => value.startsWith("c"));
          expect(own).toEqual(sent.filter((value) => value.startsWith("c")));
          // An acknowledgment names the sequence number the message arrived with, duplicate or not.
          const bySeq = new Map(arrivals.map((item) => [item.data, item.seq]));
          const ownValues = sent.filter((value) => value.startsWith("c"));
          for (const [at, ack] of results.entries()) expect(ack.seq).toBe(bySeq.get(ownValues[at]));
          expect(r.hub.stats().published).toBe(sent.length);
        } finally {
          await r.dispose();
        }
      }),
      { numRuns: 60 },
    );
  }, 60_000);
});

const memberOf = (id: string, v: number, s: number): Member => ({ id, v, s });

/** A lifecycle per member: a join, then sets with rising versions, ending in a leave or not. */
const lifecycles = fc.array(
  fc.record({
    sets: fc.integer({ min: 0, max: 4 }),
    leaves: fc.boolean(),
  }),
  { minLength: 1, maxLength: 5 },
);

describe("presence convergence", () => {
  it("[RT-G11] join and set events converge to the highest version per member under any order and any repetition", () => {
    fc.assert(
      fc.property(lifecycles, fc.nat(), (members, seed) => {
        const events: Member[] = [];
        members.forEach((item, at) => {
          for (let v = 1; v <= item.sets + 1; v += 1)
            events.push(memberOf(`m${at}`, v, v * 10 + at));
        });
        const ordered = new Map<string, Member>();
        for (const event of events) applyPresence(ordered, event.v === 1 ? "join" : "set", event);
        // A deterministic shuffle with repetitions, driven by the seed.
        const delivered: Member[] = [];
        let state = seed + 1;
        const next = (): number => {
          state = (state * 1103515245 + 12345) % 2147483648;
          return state;
        };
        const pool = [...events, ...events.filter(() => next() % 3 === 0)];
        while (pool.length > 0) delivered.push(pool.splice(next() % pool.length, 1)[0] as Member);
        const shuffled = new Map<string, Member>();
        for (const event of delivered)
          applyPresence(shuffled, event.v === 1 ? "join" : "set", event);
        expect(new Map([...shuffled].sort())).toEqual(new Map([...ordered].sort()));
      }),
      { numRuns: 400 },
    );
  });

  it("[RT-G11] with leaves after the member's events, the result is the set of members that did not leave", () => {
    fc.assert(
      fc.property(lifecycles, fc.nat(), (members, seed) => {
        const table = new Map<string, Member>();
        const stream: ["join" | "set" | "leave", Member][] = [];
        members.forEach((item, at) => {
          const id = `m${at}`;
          for (let v = 1; v <= item.sets + 1; v += 1)
            stream.push([v === 1 ? "join" : "set", memberOf(id, v, v)]);
          if (item.leaves) stream.push(["leave", memberOf(id, item.sets + 1, item.sets + 1)]);
        });
        // Interleave members at random, keeping each member's own events in order, and repeat some events.
        const queues = members.map((_item, at) =>
          stream.filter(([, member]) => member.id === `m${at}`),
        );
        let state = seed + 7;
        const next = (): number => {
          state = (state * 1103515245 + 12345) % 2147483648;
          return state;
        };
        const live = queues.filter((queue) => queue.length > 0);
        while (live.length > 0) {
          const at = next() % live.length;
          const queue = live[at] as typeof stream;
          const [op, member] = queue.shift() as (typeof stream)[number];
          applyPresence(table, op, member);
          if (next() % 4 === 0 && op !== "leave") applyPresence(table, op, member);
          if (queue.length === 0) live.splice(at, 1);
        }
        const expected = members
          .map((item, at) => ({ item, id: `m${at}` }))
          .filter(({ item }) => !item.leaves)
          .map(({ item, id }) => [id, item.sets + 1]);
        expect([...table.values()].map((member) => [member.id, member.v]).sort()).toEqual(
          expected.sort(),
        );
      }),
      { numRuns: 400 },
    );
  });

  it("[RT-G11] a sync replaces the table whatever it held, and events after it apply by version", () => {
    fc.assert(
      fc.property(
        fc.array(fc.nat({ max: 5 }), { maxLength: 6 }),
        fc.array(fc.nat({ max: 5 }), { maxLength: 6 }),
        (before, after) => {
          const table = new Map<string, Member>();
          for (const v of before) applyPresence(table, "join", memberOf(`b${v}`, 1, v));
          const snapshot = [...new Set(after)].map((v) => memberOf(`a${v}`, 2, v));
          applyPresence(table, "sync", snapshot);
          expect([...table.keys()].sort()).toEqual(snapshot.map((member) => member.id).sort());
          for (const member of snapshot)
            expect(applyPresence(table, "set", { ...member, v: 1 })).toBe(false);
        },
      ),
      { numRuns: 200 },
    );
  });
});

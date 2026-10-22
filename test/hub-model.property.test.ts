import fc from "fast-check";
import { afterEach, describe, expect, it } from "vitest";
import { decodeClientFrame, type Member, type ServerFrame } from "../src/index";
import { createHub, type Hub } from "../src/server/index";
import { connectTest, type TestConnection } from "../src/testing";

const open: Hub[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((hub) => hub.close()));
});

const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
const HISTORY = 5;
const CLIENTS = 3;

type Action =
  | { readonly k: "pub" }
  | { readonly k: "sub"; readonly client: number }
  | { readonly k: "drop"; readonly client: number };

const actions = fc.array(
  fc.oneof(
    fc.constant<Action>({ k: "pub" }),
    fc.record({ k: fc.constant("sub" as const), client: fc.integer({ min: 0, max: CLIENTS - 1 }) }),
    fc.record({
      k: fc.constant("drop" as const),
      client: fc.integer({ min: 0, max: CLIENTS - 1 }),
    }),
  ),
  { minLength: 1, maxLength: 60 },
);

describe("hub model", () => {
  it("[RT-G2][RT-G4][RT-G8] every client receives exactly what the model predicts: in order, once, resumed from its last seq", async () => {
    await fc.assert(
      fc.asyncProperty(actions, async (script) => {
        const hub = createHub({
          channels: { "m:{x}": { subscribe: true, publish: true, history: HISTORY } },
          onError: () => undefined,
        });
        open.push(hub);
        const sessions: (TestConnection | undefined)[] = Array(CLIENTS).fill(undefined);
        const received: number[][] = Array.from({ length: CLIENTS }, () => []);
        const expected: number[][] = Array.from({ length: CLIENTS }, () => []);
        const last: (number | undefined)[] = Array(CLIENTS).fill(undefined);
        let head = 0;
        const track = (client: number, connection: TestConnection): void => {
          // Reads what this session received, once the hub is idle.
          sessions[client] = connection;
        };
        const harvest = (client: number): void => {
          const connection = sessions[client];
          if (connection === undefined) return;
          for (const frame of connection.frames)
            if (frame.t === "msg" && !received[client]?.includes(frame.s))
              received[client]?.push(frame.s);
        };
        for (const action of script) {
          if (action.k === "pub") {
            head += 1;
            await hub.publish("m:1", "e", head);
            for (let client = 0; client < CLIENTS; client += 1)
              if (sessions[client] !== undefined) {
                expected[client]?.push(head);
                last[client] = head;
              }
          } else if (action.k === "sub") {
            if (sessions[action.client] !== undefined) continue;
            const connection = connectTest(hub);
            const after = last[action.client];
            connection.send({
              t: "sub",
              n: 1,
              c: "m:1",
              ...(after === undefined ? {} : { after }),
            });
            await connection.settled();
            await flush();
            track(action.client, connection);
            if (after !== undefined) {
              const oldest = Math.max(1, head - HISTORY + 1);
              for (let seq = Math.max(after + 1, oldest); seq <= head; seq += 1)
                expected[action.client]?.push(seq);
            }
            last[action.client] = head;
          } else {
            const connection = sessions[action.client];
            if (connection === undefined) continue;
            await flush();
            harvest(action.client);
            await connection.close();
            sessions[action.client] = undefined;
          }
        }
        await flush();
        await flush();
        for (let client = 0; client < CLIENTS; client += 1) harvest(client);
        for (let client = 0; client < CLIENTS; client += 1)
          expect(received[client]).toEqual(expected[client]);
      }),
      { numRuns: 120 },
    );
  });

  it("[RT-G11] after any sequence of joins, state changes, leaves and drops the watcher's member list equals the live members", async () => {
    type Op =
      | { readonly k: "join"; readonly client: number }
      | { readonly k: "set"; readonly client: number }
      | { readonly k: "leave"; readonly client: number }
      | { readonly k: "drop"; readonly client: number };
    const ops = fc.array(
      fc.record({
        k: fc.constantFrom("join", "set", "leave", "drop"),
        client: fc.integer({ min: 0, max: 3 }),
      }) as fc.Arbitrary<Op>,
      { minLength: 1, maxLength: 50 },
    );
    await fc.assert(
      fc.asyncProperty(ops, async (script) => {
        const hub = createHub({
          channels: { "p:{x}": { subscribe: true, presence: true } },
          onError: () => undefined,
        });
        open.push(hub);
        const watcher = connectTest(hub);
        watcher.send({ t: "sub", n: 1, c: "p:1", watch: true });
        await watcher.settled();
        const clients: (TestConnection | undefined)[] = Array(4).fill(undefined);
        const joined = new Set<number>();
        for (const op of script) {
          const connection = clients[op.client];
          if (op.k === "join") {
            if (connection !== undefined && joined.has(op.client)) continue;
            const next = connection ?? connectTest(hub, { user: `u${op.client}` });
            clients[op.client] = next;
            next.send({ t: "sub", n: 1, c: "p:1", presence: { v: 0 } });
            joined.add(op.client);
            await next.settled();
          } else if (op.k === "set") {
            if (connection === undefined || !joined.has(op.client)) continue;
            connection.send({ t: "pres", c: "p:1", s: { v: Math.floor(Math.random() * 1000) } });
            await connection.settled();
          } else if (op.k === "leave") {
            if (connection === undefined || !joined.has(op.client)) continue;
            connection.send({ t: "unsub", c: "p:1" });
            joined.delete(op.client);
            await connection.settled();
          } else if (connection !== undefined) {
            await connection.close();
            clients[op.client] = undefined;
            joined.delete(op.client);
          }
          await flush();
        }
        await flush();
        const members = new Map<string, Member>();
        for (const frame of watcher.frames as ServerFrame[]) {
          if (frame.t !== "pres") continue;
          if (frame.op === "sync") {
            members.clear();
            for (const member of frame.m as readonly Member[]) members.set(member.id, member);
          } else {
            const member = frame.m as Member;
            const known = members.get(member.id);
            if (frame.op === "leave") members.delete(member.id);
            else if (known === undefined || known.v < member.v) members.set(member.id, member);
          }
        }
        const live = [...joined].map((client) => clients[client]?.id).sort();
        expect([...members.keys()].sort()).toEqual(live);
      }),
      { numRuns: 80 },
    );
  });
});

describe("envelope fuzz", () => {
  it("[RT-R6] decodeClientFrame never throws, and an accepted frame has valid names", () => {
    const text = fc.oneof(
      fc.string(),
      fc.json(),
      fc.anything().map((value) => {
        try {
          return JSON.stringify(value) ?? "undefined";
        } catch {
          return "{}";
        }
      }),
      fc
        .record({
          t: fc.constantFrom("hello", "ping", "sub", "unsub", "pub", "req", "pres", "x"),
          n: fc.oneof(fc.nat(), fc.string(), fc.constant(null)),
          c: fc.oneof(fc.string(), fc.constant("chat:lobby"), fc.constant(null)),
          e: fc.oneof(fc.string(), fc.constant("message")),
          d: fc.anything(),
          after: fc.oneof(fc.nat(), fc.integer(), fc.string()),
        })
        .map((value) => JSON.stringify(value)),
    );
    fc.assert(
      fc.property(text, (input) => {
        const result = decodeClientFrame(input, 65_536);
        if (!result.ok) {
          expect(["bad_frame", "bad_channel", "too_large"]).toContain(result.code);
          return;
        }
        const frame = result.frame;
        if ("c" in frame) expect(frame.c).toMatch(/^[A-Za-z0-9_.:/@-]{1,128}$/);
        if (frame.t === "pub") expect(() => JSON.parse(frame.d)).not.toThrow();
        if (frame.t === "sub" && frame.after !== undefined)
          expect(Number.isSafeInteger(frame.after) && frame.after >= 0).toBe(true);
      }),
      { numRuns: 3000 },
    );
  });

  it("[RT-G13] garbage on a connection never throws and leaves the hub serving other connections", async () => {
    const hub = createHub({
      channels: { "g:{x}": { subscribe: true, publish: true } },
      onError: () => undefined,
    });
    open.push(hub);
    const good = connectTest(hub);
    good.send({ t: "sub", n: 1, c: "g:1" });
    await good.settled();
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.oneof(fc.string(), fc.json()), { maxLength: 20 }),
        async (garbage) => {
          const bad = connectTest(hub);
          for (const item of garbage) bad.send(item);
          await bad.settled();
          await bad.close();
        },
      ),
      { numRuns: 200 },
    );
    await hub.publish("g:1", "e", 1);
    expect(good.frames.filter((frame) => frame.t === "msg")).toHaveLength(1);
    expect(hub.stats().connections).toBe(1);
  });
});

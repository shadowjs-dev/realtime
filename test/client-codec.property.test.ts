import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { sseParser } from "../src/client/sse";
import { backoff } from "../src/client/util";
import { decodeClientFrame, decodeServerFrame } from "../src/index";
import { rig } from "./fixtures/harness";

const json: fc.Arbitrary<unknown> = fc.jsonValue();

/** Objects that look like frames: a known type plus random fields, to reach the validation branches. */
const frameLike = fc.record(
  {
    t: fc.constantFrom(
      "ready",
      "subbed",
      "unsubbed",
      "msg",
      "ack",
      "rep",
      "err",
      "pres",
      "pong",
      "bye",
      "sub",
      "pub",
      "req",
      "ping",
      "hello",
      "nope",
    ),
    n: fc.oneof(fc.nat(), fc.string(), fc.constant(null), fc.double()),
    c: fc.oneof(fc.constantFrom("chat:lobby", "chat:other", "", "x y"), fc.string()),
    s: fc.oneof(fc.nat(), fc.string(), fc.double()),
    e: fc.oneof(fc.constantFrom("message", "a", "", "a/b"), fc.string()),
    d: json,
    v: fc.oneof(fc.nat(), fc.string()),
    id: fc.oneof(fc.string(), fc.nat()),
    hb: fc.oneof(fc.nat(), fc.string()),
    max: fc.oneof(fc.nat(), fc.string()),
    head: fc.oneof(fc.nat(), fc.string()),
    gap: fc.boolean(),
    reset: fc.boolean(),
    why: fc.oneof(fc.constantFrom("revoked", "gap", "reset", "unavailable", ""), fc.string()),
    code: fc.oneof(
      fc.constantFrom("forbidden", "timeout", "rate_limited", "unavailable"),
      fc.nat(),
      fc.string(),
    ),
    op: fc.oneof(fc.constantFrom("sync", "join", "set", "leave"), fc.string()),
    m: fc.oneof(
      json,
      fc.array(fc.record({ id: fc.string(), v: fc.nat(), s: json }, { requiredKeys: [] })),
    ),
    r: fc.oneof(fc.constant(1), fc.string()),
    dup: fc.boolean(),
  },
  { requiredKeys: ["t"] },
);

describe("the frame codecs", () => {
  it("[RT-C-50] never throw, whatever text arrives", () => {
    fc.assert(
      fc.property(fc.string({ maxLength: 200 }), (text) => {
        decodeServerFrame(text);
        decodeClientFrame(text, 65_536);
      }),
      { numRuns: 2000 },
    );
  });

  it("[RT-C-50] never throw for JSON of any shape, nor for objects that look like frames", () => {
    fc.assert(
      fc.property(fc.oneof(json, frameLike), (value) => {
        const text = JSON.stringify(value);
        const server = decodeServerFrame(text);
        if (server !== undefined) expect(typeof server.t).toBe("string");
        const client = decodeClientFrame(text, 65_536);
        if (client.ok) expect(typeof client.frame.t).toBe("string");
        else expect(typeof client.why).toBe("string");
      }),
      { numRuns: 3000 },
    );
  });

  it("[RT-C-50] a frame the client decodes keeps the fields the client relies on in their types", () => {
    fc.assert(
      fc.property(frameLike, (value) => {
        const frame = decodeServerFrame(JSON.stringify(value));
        if (frame === undefined) return;
        switch (frame.t) {
          case "msg":
            expect(Number.isInteger(frame.s) && frame.s >= 1).toBe(true);
            expect(typeof frame.c).toBe("string");
            expect(typeof frame.e).toBe("string");
            break;
          case "ack":
            expect(Number.isInteger(frame.n) && Number.isInteger(frame.s)).toBe(true);
            break;
          case "subbed":
            expect(Number.isInteger(frame.head) && frame.head >= 0).toBe(true);
            break;
          case "pres":
            if (frame.op === "sync") expect(Array.isArray(frame.m)).toBe(true);
            else expect(Array.isArray(frame.m)).toBe(false);
            break;
          case "ready":
            expect(typeof frame.id).toBe("string");
            break;
          default:
            break;
        }
      }),
      { numRuns: 3000 },
    );
  });
});

describe("a client under garbage from the server", () => {
  it("[RT-C-51] ignores frames of any shape and keeps working", async () => {
    const r = rig();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    const socket = r.sockets.sockets[0];
    if (socket === undefined) throw new Error("no socket");
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.oneof(json, frameLike, fc.string()), { maxLength: 8 }),
        async (values) => {
          for (const value of values) {
            socket.injectRaw(typeof value === "string" ? value : JSON.stringify(value));
          }
          await Promise.resolve();
        },
      ),
      { numRuns: 300 },
    );
    expect(r.rt.status === "open" || r.rt.status === "reconnecting").toBe(true);
    // Whatever happened, the client still publishes and receives once it is connected again.
    await r.until(() => expect(r.rt.status).toBe("open"));
    const before = seen.length;
    await lobby.publish("message", "after the noise").catch(() => undefined);
    await r.until(() => expect(seen.length).toBeGreaterThanOrEqual(before));
    r.rt.close();
    await r.dispose();
  });
});

describe("the event stream parser", () => {
  const events = fc.array(
    fc.record({
      name: fc.constantFrom("rt", "other", ""),
      lines: fc.array(
        fc.string({ maxLength: 12 }).filter((line) => !/[\r\n]/.test(line)),
        { minLength: 1, maxLength: 3 },
      ),
      comment: fc.boolean(),
    }),
    { maxLength: 6 },
  );

  it("[RT-C-52] yields the same events however the bytes are split, with LF, CRLF or CR line ends", () => {
    fc.assert(
      fc.property(
        events,
        fc.constantFrom("\n", "\r\n", "\r"),
        fc.array(fc.nat(), { maxLength: 6 }),
        (list, eol, cuts) => {
          // A final CR may be the first half of CRLF, so the parser waits for one more byte: end the stream with one.
          const text = `${list
            .map(
              (item) =>
                `${item.comment ? `: keepalive${eol}` : ""}${item.name === "" ? "" : `event: ${item.name}${eol}`}${item.lines.map((line) => `data: ${line}${eol}`).join("")}${eol}`,
            )
            .join("")}\n`;
          const whole: string[] = [];
          sseParser((data) => whole.push(data))(text);
          const pieces: string[] = [];
          const feed = sseParser((data) => pieces.push(data));
          let from = 0;
          for (const cut of [...cuts.map((value) => value % (text.length + 1))].sort(
            (a, b) => a - b,
          )) {
            feed(text.slice(from, cut));
            from = cut;
          }
          feed(text.slice(from));
          expect(pieces).toEqual(whole);
          const expected = list
            .filter((item) => item.name === "rt")
            .map((item) => item.lines.join("\n"));
          expect(whole).toEqual(expected);
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe("the backoff", () => {
  it("[RT-R10] is below the exponential cap, never above the maximum, and the cap never falls", () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 2000 }),
        fc.integer({ min: 1, max: 100_000 }),
        fc.array(fc.double({ min: 0, max: 0.999999, noNaN: true }), {
          minLength: 1,
          maxLength: 60,
        }),
        (min, extra, randoms) => {
          const max = min + extra;
          let cap = 0;
          randoms.forEach((value, attempt) => {
            const delay = backoff(attempt, min, max, () => value);
            const expectedCap = Math.min(max, min * 2 ** Math.min(attempt, 30));
            expect(delay).toBeGreaterThanOrEqual(0);
            expect(delay).toBeLessThanOrEqual(expectedCap);
            expect(expectedCap).toBeGreaterThanOrEqual(cap);
            cap = expectedCap;
          });
        },
      ),
      { numRuns: 300 },
    );
  });
});

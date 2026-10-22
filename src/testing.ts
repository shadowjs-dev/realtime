import type { Adapter, PublishOptions } from "./adapter";
import {
  decodeServerFrame,
  type Json,
  type Member,
  type ServerFrame,
  type StoredEntry,
} from "./protocol";
import type { Hub } from "./server/index";
import type { Identity } from "./server/rules";

/**
 * A connection to a hub with no network: frames go in as objects and come out decoded. For tests of an app's channel rules and
 * request handlers.
 * @since 1.0.0
 */
export interface TestConnection {
  /** The connection id the hub assigned. */
  readonly id: string;
  /** Every frame the hub sent, decoded, in order. */
  readonly frames: readonly ServerFrame[];
  /** The code and reason the hub closed the connection with, once it did. */
  readonly closedWith: { readonly code: number; readonly reason: string } | undefined;
  /** Sends one frame (an object, or raw text to test malformed input). */
  send(frame: Readonly<Record<string, unknown>> | string): void;
  /** Resolves with the first frame (already received or yet to come) that satisfies `match`; rejects after `timeoutMs`. */
  until<T extends ServerFrame>(
    match: (frame: ServerFrame) => frame is T,
    timeoutMs?: number,
  ): Promise<T>;
  /** Resolves when the hub handled every frame sent so far. */
  settled(): Promise<void>;
  /** Closes the connection as a network drop would. */
  close(): Promise<void>;
}

/**
 * Opens a {@link TestConnection} to `hub`.
 *
 * @param hub The hub.
 * @param identity Who the connection is.
 * @returns The connection (its `ready` frame has been received).
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 * import { connectTest } from "@shadow-js/realtime/testing";
 *
 * const hub = createHub({ channels: { "chat:{room}": { subscribe: true } } });
 * const client = connectTest(hub);
 * client.send({ t: "sub", n: 1, c: "chat:lobby" });
 * await client.settled();
 * client.frames.some((frame) => frame.t === "subbed"); // true
 * await hub.close();
 * @since 1.0.0
 */
export function connectTest(hub: Hub, identity: Identity = {}): TestConnection {
  const frames: ServerFrame[] = [];
  const watchers: ((frame: ServerFrame) => void)[] = [];
  let closedWith: { code: number; reason: string } | undefined;
  const connection = hub.connect(
    {
      send(text) {
        if (closedWith !== undefined) return false;
        const frame = decodeServerFrame(text);
        if (frame === undefined) return true;
        frames.push(frame);
        for (const watcher of [...watchers]) watcher(frame);
        return true;
      },
      close(code, reason) {
        closedWith = { code, reason };
      },
    },
    identity,
  );
  return {
    id: connection.id,
    frames,
    get closedWith() {
      return closedWith;
    },
    send(frame) {
      connection.receive(typeof frame === "string" ? frame : JSON.stringify(frame));
    },
    until<T extends ServerFrame>(match: (frame: ServerFrame) => frame is T, timeoutMs = 2000) {
      const found = frames.find(match);
      if (found !== undefined) return Promise.resolve(found);
      return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => {
          watchers.splice(watchers.indexOf(watcher), 1);
          reject(new Error("no matching frame arrived in time"));
        }, timeoutMs);
        const watcher = (frame: ServerFrame): void => {
          if (!match(frame)) return;
          clearTimeout(timer);
          watchers.splice(watchers.indexOf(watcher), 1);
          resolve(frame);
        };
        watchers.push(watcher);
      });
    },
    settled: () => connection.settled(),
    async close() {
      connection.closed();
      await connection.done;
    },
  };
}

/**
 * A `WebSocket` constructor whose other end is `hub` in the same process: pass it as the `WebSocket` option of `createRealtime` to
 * test client code and server rules together with no network and no server. The hub is real; only the socket is replaced.
 *
 * @param hub The hub the sockets connect to.
 * @param identity Who the connections are.
 * @returns A drop-in for the global `WebSocket`.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 * import { createHub } from "@shadow-js/realtime/server";
 * import { memoryWebSocket } from "@shadow-js/realtime/testing";
 *
 * const hub = createHub({ channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * const rt = createRealtime({ url: "/rt", transport: "ws", WebSocket: memoryWebSocket(hub) });
 * rt.close();
 * void hub.close();
 * @since 1.0.0
 */
export function memoryWebSocket(hub: Hub, identity: Identity = {}): typeof WebSocket {
  class MemorySocket {
    onopen: ((event: Event) => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onclose: ((event: CloseEvent) => void) | null = null;
    onerror: ((event: Event) => void) | null = null;
    readyState = 0;
    readonly url: string;
    #connection: ReturnType<Hub["connect"]> | undefined;

    constructor(url: string) {
      this.url = url;
      queueMicrotask(() => {
        if (this.readyState !== 0) return;
        this.readyState = 1;
        this.#connection = hub.connect(
          {
            send: (frame) => {
              if (this.readyState !== 1) return false;
              queueMicrotask(() => {
                if (this.readyState === 1)
                  this.onmessage?.(new MessageEvent("message", { data: frame }));
              });
              return true;
            },
            close: (code, reason) => this.#finish(code, reason),
          },
          identity,
        );
        this.onopen?.(new Event("open"));
      });
    }

    send(text: string): void {
      if (this.readyState !== 1)
        throw new DOMException("the socket is not open", "InvalidStateError");
      const connection = this.#connection;
      queueMicrotask(() => connection?.receive(text));
    }

    close(code = 1000): void {
      if (this.readyState < 2) this.#finish(code, "");
    }

    #finish(code: number, reason: string): void {
      if (this.readyState === 3) return;
      this.readyState = 3;
      this.#connection?.closed();
      const event = { type: "close", code, reason, wasClean: code !== 1006 } as CloseEvent;
      queueMicrotask(() => this.onclose?.(event));
    }
  }
  return MemorySocket as unknown as typeof WebSocket;
}

/** The test runner the conformance suite registers its tests with (Vitest's `describe`, `it`, `expect` fit). @since 1.0.0 */
export interface ConformanceRunner {
  describe(name: string, body: () => void): void;
  it(name: string, body: () => Promise<void>, timeoutMs?: number): void;
  expect(value: unknown): {
    toBe(expected: unknown): void;
    toEqual(expected: unknown): void;
    toBeGreaterThan(expected: number): void;
    toHaveLength(expected: number): void;
    toBeLessThanOrEqual(expected: number): void;
  };
  /** Runs after each test (registers cleanup). */
  afterEach(body: () => Promise<void>): void;
}

/** Options of {@link adapterConformance}. @since 1.0.0 */
export interface ConformanceOptions {
  readonly runner: ConformanceRunner;
  /**
   * Returns an adapter attached to `namespace`. Two calls with the same namespace are two servers sharing one store (for a
   * Redis adapter: two clients on one key prefix); different namespaces share nothing. `now` is not passed: the suite uses
   * short real timeouts.
   */
  readonly connect: (namespace: string) => Adapter | Promise<Adapter>;
}

const WAIT_STEP_MS = 5;

async function until(condition: () => boolean, ms = 3000): Promise<void> {
  const stop = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > stop) throw new Error("the condition did not become true in time");
    await new Promise((resolve) => setTimeout(resolve, WAIT_STEP_MS));
  }
}

/**
 * Registers the tests every adapter must pass (https://realtime.shadowjs.net/guides/realtime-protocol/): atomic gapless numbering, ordered fan-out
 * between servers, retention, idempotency, and presence with exactly-once leave. Run it against your own adapter.
 *
 * @param options The test runner and a way to open adapters on a shared store.
 * @example
 * import { describe, expect, it, afterEach } from "vitest";
 * import { memoryAdapter } from "@shadow-js/realtime/server";
 * import { adapterConformance } from "@shadow-js/realtime/testing";
 *
 * const stores = new Map<string, ReturnType<typeof memoryAdapter>>();
 * adapterConformance({
 *   runner: { describe, it, expect, afterEach },
 *   connect: (namespace) => {
 *     const found = stores.get(namespace) ?? memoryAdapter();
 *     stores.set(namespace, found);
 *     return found;
 *   },
 * });
 * @since 1.0.0
 */
export function adapterConformance(options: ConformanceOptions): void {
  const { runner } = options;
  const { describe, it, expect } = runner;
  let counter = 0;
  const opened: Adapter[] = [];
  const fresh = (): string => {
    counter += 1;
    return `conformance-${Date.now().toString(36)}-${counter}`;
  };
  const open = async (namespace: string): Promise<Adapter> => {
    const adapter = await options.connect(namespace);
    opened.push(adapter);
    return adapter;
  };
  runner.afterEach(async () => {
    await Promise.all(opened.splice(0).map((adapter) => adapter.close().catch(() => undefined)));
  });
  const rules: PublishOptions = { history: 100, historyTtlMs: 60_000, idempotencyTtlMs: 60_000 };
  const entry = (n: number): StoredEntry => ({ event: "e", data: String(n) });

  describe("adapter: messages", () => {
    it("[RT-AD-01] numbers messages 1, 2, 3 per channel, independently", async () => {
      const adapter = await open(fresh());
      const seqs: number[] = [];
      for (let at = 0; at < 3; at += 1)
        seqs.push((await adapter.publish("a", entry(at), rules)).seq);
      expect(seqs).toEqual([1, 2, 3]);
      expect((await adapter.publish("b", entry(0), rules)).seq).toBe(1);
    });

    it("[RT-AD-02] delivers to listeners in seq order, and stops after unsubscribe", async () => {
      const adapter = await open(fresh());
      const seen: number[] = [];
      const stop = await adapter.subscribe("a", (message) => seen.push(message.seq));
      for (let at = 0; at < 20; at += 1) await adapter.publish("a", entry(at), rules);
      await until(() => seen.length === 20);
      expect(seen).toEqual(Array.from({ length: 20 }, (_, at) => at + 1));
      await stop();
      await adapter.publish("a", entry(99), rules);
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(seen).toHaveLength(20);
    });

    it("[RT-AD-03] numbers concurrent publishes from two servers without a duplicate or a gap, seen in order on both", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const seenFirst: number[] = [];
      const seenSecond: number[] = [];
      await first.subscribe("a", (message) => seenFirst.push(message.seq));
      await second.subscribe("a", (message) => seenSecond.push(message.seq));
      const results = await Promise.all(
        Array.from({ length: 100 }, (_, at) =>
          (at % 2 === 0 ? first : second).publish("a", entry(at), rules),
        ),
      );
      expect(results.map((result) => result.seq).sort((x, y) => x - y)).toEqual(
        Array.from({ length: 100 }, (_, at) => at + 1),
      );
      await until(() => seenFirst.length === 100 && seenSecond.length === 100);
      expect(seenFirst).toEqual(Array.from({ length: 100 }, (_, at) => at + 1));
      expect(seenSecond).toEqual(Array.from({ length: 100 }, (_, at) => at + 1));
    });

    it("[RT-AD-04] reads history after a seq with a limit, and reports head and oldest", async () => {
      const adapter = await open(fresh());
      for (let at = 1; at <= 10; at += 1) await adapter.publish("a", entry(at), rules);
      const page = await adapter.history("a", 3, 4);
      expect(page.messages.map((message) => message.seq)).toEqual([4, 5, 6, 7]);
      expect(page.head).toBe(10);
      expect(page.oldest).toBe(1);
      expect((await adapter.history("nothing", 0, 10)).head).toBe(0);
      expect((await adapter.history("a", Number.MAX_SAFE_INTEGER, 0)).head).toBe(10);
    });

    it("[RT-AD-05] keeps only the newest `history` messages", async () => {
      const adapter = await open(fresh());
      const small: PublishOptions = { ...rules, history: 3 };
      for (let at = 1; at <= 8; at += 1) await adapter.publish("a", entry(at), small);
      const page = await adapter.history("a", 0, 100);
      expect(page.messages.map((message) => message.seq)).toEqual([6, 7, 8]);
      expect(page.oldest).toBe(6);
      expect(page.head).toBe(8);
    });

    it("[RT-AD-06] forgets messages older than historyTtlMs but keeps numbering", async () => {
      const adapter = await open(fresh());
      // 400 ms: an adapter over a network (Redis, a Durable Object in another location) takes milliseconds per call, which a shorter lifetime
      // would count against the message it has just stored.
      const short: PublishOptions = { ...rules, historyTtlMs: 400 };
      await adapter.publish("a", entry(1), short);
      await new Promise((resolve) => setTimeout(resolve, 700));
      await adapter.publish("a", entry(2), short);
      const page = await adapter.history("a", 0, 100);
      expect(page.messages.map((message) => message.seq)).toEqual([2]);
      expect(page.head).toBe(2);
    });

    it("[RT-AD-07] with history 0 retains nothing but still numbers", async () => {
      const adapter = await open(fresh());
      const none: PublishOptions = { ...rules, history: 0 };
      expect((await adapter.publish("a", entry(1), none)).seq).toBe(1);
      expect((await adapter.publish("a", entry(2), none)).seq).toBe(2);
      const page = await adapter.history("a", 0, 10);
      expect(page.messages).toHaveLength(0);
      expect(page.head).toBe(2);
      expect(page.oldest).toBe(0);
    });

    it("[RT-AD-08] accepts a publish with a known idempotency key once, across servers, until the key expires", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const keyed: PublishOptions = { ...rules, key: "k1", idempotencyTtlMs: 400 };
      const one = await first.publish("a", entry(1), keyed);
      const two = await second.publish("a", entry(2), keyed);
      expect(one).toEqual({ seq: 1, duplicate: false });
      expect(two).toEqual({ seq: 1, duplicate: true });
      expect((await first.history("a", 0, 10)).messages).toHaveLength(1);
      await new Promise((resolve) => setTimeout(resolve, 700));
      expect((await first.publish("a", entry(3), keyed)).duplicate).toBe(false);
    });

    it("[RT-AD-09] gives two racing publishes with one key a single seq", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const keyed: PublishOptions = { ...rules, key: "race" };
      const results = await Promise.all([
        first.publish("a", entry(1), keyed),
        second.publish("a", entry(2), keyed),
      ]);
      expect(results.filter((result) => result.duplicate)).toHaveLength(1);
      expect(results[0]?.seq).toBe(results[1]?.seq);
    });

    it("[RT-AD-10] isolates namespaces", async () => {
      const one = await open(fresh());
      const two = await open(fresh());
      await one.publish("a", entry(1), rules);
      expect((await two.history("a", 0, 10)).head).toBe(0);
    });
  });

  describe("adapter: presence", () => {
    const member = (id: string, v = 1, s: Json = null): Member => ({ id, v, s });

    it("[RT-AD-20] lists, updates and removes members and announces join, set and leave to every server", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const events: string[] = [];
      await second.presenceSubscribe("room", (event) =>
        events.push(`${event.op}:${event.member.id}:${event.member.v}`),
      );
      await first.presenceSet("room", member("m1"), 5000);
      await first.presenceSet("room", member("m1", 2, { x: 1 }), 5000);
      await first.presenceSet("room", member("m2"), 5000);
      expect((await second.presenceList("room")).map((item) => item.id).sort()).toEqual([
        "m1",
        "m2",
      ]);
      expect(await first.presenceRemove("room", "m1")).toBe(true);
      await until(() => events.length === 4);
      expect(events).toEqual(["join:m1:1", "set:m1:2", "join:m2:1", "leave:m1:2"]);
    });

    it("[RT-AD-21] ignores an older version of a member", async () => {
      const adapter = await open(fresh());
      await adapter.presenceSet("room", member("m1", 5, "new"), 5000);
      await adapter.presenceSet("room", member("m1", 3, "old"), 5000);
      expect((await adapter.presenceList("room"))[0]).toEqual(member("m1", 5, "new"));
    });

    it("[RT-AD-22] removes a member once even when two servers race to remove it", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const leaves: string[] = [];
      await first.presenceSubscribe(
        "room",
        (event) => event.op === "leave" && leaves.push(event.member.id),
      );
      await first.presenceSet("room", member("m1"), 5000);
      const results = await Promise.all([
        first.presenceRemove("room", "m1"),
        second.presenceRemove("room", "m1"),
      ]);
      expect(results.filter(Boolean)).toHaveLength(1);
      await until(() => leaves.length >= 1);
      await new Promise((resolve) => setTimeout(resolve, 40));
      expect(leaves).toEqual(["m1"]);
    });

    it("[RT-AD-23] expires members without a heartbeat, and reap announces each leave once across servers", async () => {
      const namespace = fresh();
      const first = await open(namespace);
      const second = await open(namespace);
      const leaves: string[] = [];
      await first.presenceSubscribe(
        "room",
        (event) => event.op === "leave" && leaves.push(event.member.id),
      );
      await first.presenceSet("room", member("gone"), 60);
      await first.presenceSet("room", member("stays"), 5000);
      await new Promise((resolve) => setTimeout(resolve, 120));
      expect((await first.presenceList("room")).map((item) => item.id)).toEqual(["stays"]);
      const reaped = await Promise.all([first.presenceReap("room"), second.presenceReap("room")]);
      expect(reaped.flat().map((item) => item.id)).toEqual(["gone"]);
      await until(() => leaves.length >= 1);
      await new Promise((resolve) => setTimeout(resolve, 40));
      expect(leaves).toEqual(["gone"]);
    });

    it("[RT-AD-24] touch extends the life of members and reports the ones that no longer exist", async () => {
      const adapter = await open(fresh());
      await adapter.presenceSet("room", member("m1"), 80);
      const missing = await adapter.presenceTouch(
        [
          { channel: "room", id: "m1" },
          { channel: "room", id: "ghost" },
        ],
        5000,
      );
      expect(missing).toEqual([{ channel: "room", id: "ghost" }]);
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect((await adapter.presenceList("room")).map((item) => item.id)).toEqual(["m1"]);
    });

    it("[RT-AD-25] stops presence events after unsubscribe", async () => {
      const adapter = await open(fresh());
      const events: string[] = [];
      const stop = await adapter.presenceSubscribe("room", (event) => events.push(event.op));
      await adapter.presenceSet("room", member("m1"), 5000);
      await until(() => events.length === 1);
      await stop();
      await adapter.presenceSet("room", member("m2"), 5000);
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(events).toEqual(["join"]);
    });
  });
}

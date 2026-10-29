import { afterEach, describe, expect, it, vi } from "vitest";
import { openStream, sseParser } from "../src/client/sse";
import { RealtimeError } from "../src/errors";
import type { ServerFrame } from "../src/protocol";
import { type Adapter, createHub, type Hub, memoryAdapter } from "../src/server/index";
import { connectTest } from "../src/testing";

const hubs: Hub[] = [];
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
});
const quiet = (): void => undefined;
const rules = { "p:{x}": { subscribe: true, presence: true, publish: true } };

const errorCodes = (frames: readonly ServerFrame[]): string[] =>
  frames.flatMap((frame) => (frame.t === "err" ? [frame.code] : []));

describe("the event stream parser is bounded", () => {
  it("[RT-R2-1] a line that never ends is refused once it passes the cap", () => {
    const feed = sseParser(
      () => undefined,
      () => 1024,
    );
    feed("data: ");
    expect(() => feed("x".repeat(2000))).toThrowError(RealtimeError);
  });

  it("[RT-R2-1] an event whose data lines pile up without a blank line is refused too", () => {
    const feed = sseParser(
      () => undefined,
      () => 1024,
    );
    let thrown: unknown;
    try {
      for (let at = 0; at < 200; at += 1) feed("data: 0123456789\n");
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(RealtimeError);
  });

  it("[RT-R2-1] lines and events under the cap still parse, split anywhere", () => {
    const seen: string[] = [];
    const feed = sseParser(
      (data) => seen.push(data),
      () => 1024,
    );
    const text = `event: rt\ndata: ${"a".repeat(500)}\n\nevent: rt\ndata: two\n\n`;
    for (let at = 0; at < text.length; at += 7) feed(text.slice(at, at + 7));
    expect(seen).toEqual(["a".repeat(500), "two"]);
  });

  it("[RT-R2-1] a stream that sends one endless line is cut: the transport reports closed and stops reading", async () => {
    let pulled = 0;
    const chunk = new TextEncoder().encode("x".repeat(65_536));
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulled += chunk.length;
        if (pulled > 200_000_000) controller.close();
        else controller.enqueue(chunk);
      },
    });
    const closed: unknown[] = [];
    openStream(
      "http://localhost/rt",
      async () => new Response(body, { status: 200 }),
      () => undefined,
      {
        rx: () => undefined,
        frame: () => undefined,
        closed: (info) => closed.push(info),
      },
    );
    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(pulled).toBeLessThan(10_000_000);
  });
});

describe("hub frame limits", () => {
  it("[RT-R2-2] subscribe and unsubscribe frames spend from a bucket, so churn is refused", async () => {
    const hub = createHub({ channels: rules, onError: quiet, now: () => 1_000 });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    for (let at = 0; at < 120; at += 1)
      client.send(at % 2 === 0 ? { t: "sub", n: at + 1, c: "p:1" } : { t: "unsub", c: "p:1" });
    await client.settled();
    const codes = errorCodes(client.frames);
    expect(codes.filter((code) => code === "rate_limited").length).toBeGreaterThan(0);
    expect(client.frames.filter((frame) => frame.t === "subbed").length).toBeLessThanOrEqual(32);
  });

  it("[RT-R2-2] a connection can still declare its full channel allowance at once", async () => {
    const hub = createHub({
      channels: rules,
      onError: quiet,
      now: () => 1_000,
      limits: { maxChannelsPerConnection: 64 },
    });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    for (let at = 0; at < 64; at += 1) client.send({ t: "sub", n: at + 1, c: `p:${at}` });
    await client.settled();
    expect(client.frames.filter((frame) => frame.t === "subbed")).toHaveLength(64);
    expect(errorCodes(client.frames)).toEqual([]);
  });

  it("[RT-R2-3] an oversized frame is refused when it arrives, not after the frames before it", async () => {
    const hub = createHub({ channels: rules, onError: quiet, limits: { maxMessageBytes: 1024 } });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    client.send({ t: "ping" });
    client.send({ t: "ping" });
    client.send(`{"t":"pub","c":"p:1","e":"m","d":"${"x".repeat(2000)}"}`);
    // Nothing was awaited: the answer is already there, so the frame never sat in the queue.
    expect(errorCodes(client.frames)).toEqual(["too_large"]);
    await client.settled();
  });

  it("[RT-R2-3] a flood of oversized frames closes the connection as invalid, never as a full queue", async () => {
    const hub = createHub({ channels: rules, onError: quiet, limits: { maxMessageBytes: 1024 } });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    client.send({ t: "ping" });
    const big = `{"t":"pub","c":"p:1","e":"m","d":"${"x".repeat(2000)}"}`;
    for (let at = 0; at < 300; at += 1) client.send(big);
    expect(client.closedWith?.code).toBe(1008);
  });
});

describe("presence over the stream transport", () => {
  it("[RT-R2-4] a state posted over HTTP is what the heartbeat writes back after the entry expired", async () => {
    vi.useFakeTimers();
    const base = memoryAdapter();
    const hub = createHub({
      adapter: base,
      channels: rules,
      limits: { presenceTtlMs: 90, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    client.send({ t: "sub", n: 1, c: "p:1", presence: { v: "first" } });
    await vi.advanceTimersByTimeAsync(5);
    await client.settled();
    await hub.setMemberState({ user: "ann" }, "p:1", client.id, { v: "second" });
    await hub.setMemberState({ user: "ann" }, "p:1", client.id, { v: "third" });
    const before = (await base.presenceList("p:1"))[0];
    expect(before?.v).toBe(3);
    // The entry is lost (an expiry, a flushed store): the next heartbeat writes the member back.
    await base.presenceRemove("p:1", client.id);
    await vi.advanceTimersByTimeAsync(40);
    const after = (await base.presenceList("p:1"))[0];
    expect(after?.s).toEqual({ v: "third" });
    expect(after?.v).toBeGreaterThanOrEqual(3);
  });

  it("[RT-R2-4] a state set by another server's POST reaches this server's connection through the presence events", async () => {
    vi.useFakeTimers();
    const base = memoryAdapter();
    const owner = createHub({
      adapter: base,
      channels: rules,
      limits: { presenceTtlMs: 90, presenceHeartbeatMs: 30 },
      onError: quiet,
    });
    const other = createHub({ adapter: base, channels: rules, onError: quiet });
    hubs.push(owner, other);
    const client = connectTest(owner, { user: "ann" });
    client.send({ t: "sub", n: 1, c: "p:1", presence: { v: "first" } });
    await vi.advanceTimersByTimeAsync(5);
    await client.settled();
    await other.setMemberState({ user: "ann" }, "p:1", client.id, { v: "remote" });
    await vi.advanceTimersByTimeAsync(1);
    await base.presenceRemove("p:1", client.id);
    await vi.advanceTimersByTimeAsync(40);
    const after = (await base.presenceList("p:1"))[0];
    expect(after?.s).toEqual({ v: "remote" });
    expect(after?.v).toBeGreaterThanOrEqual(2);
  });
});

describe("errors on the way out are reported", () => {
  it("[RT-R2-6] a transport that throws while it is dropped reports both failures", async () => {
    const seen: { where: string; connection?: string }[] = [];
    const hub = createHub({
      channels: {},
      onError: (_error, context) => {
        seen.push({
          where: context.where,
          ...(context.connection === undefined ? {} : { connection: context.connection }),
        });
      },
    });
    hubs.push(hub);
    let sends = 0;
    const connection = hub.connect({
      send: () => {
        sends += 1;
        if (sends > 1) throw new Error("gone");
        return true;
      },
      close: () => {
        throw new Error("gone too");
      },
    });
    await hub.close();
    await connection.done;
    expect(seen).toEqual([
      { where: "transport.send", connection: connection.id },
      { where: "transport.close", connection: connection.id },
    ]);
  });

  it("[RT-R2-6] a memory adapter listener that throws is reported, and the others still hear the message", async () => {
    const seen: { error: unknown; where: string; channel?: string }[] = [];
    const adapter = memoryAdapter({
      onError: (error, context) => seen.push({ error, ...context }),
    });
    const heard: number[] = [];
    await adapter.subscribe("c", () => {
      throw new Error("listener bug");
    });
    await adapter.subscribe("c", (message) => heard.push(message.seq));
    await adapter.publish(
      "c",
      { event: "e", data: "1" },
      { history: 1, historyTtlMs: 1000, idempotencyTtlMs: 1000 },
    );
    expect(heard).toEqual([1]);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.where).toBe("listener");
    expect(seen[0]?.channel).toBe("c");
    expect(String(seen[0]?.error)).toContain("listener bug");
    await adapter.close();
  });

  it("[RT-R2-6] a memory adapter without a reporter logs a throwing listener instead of swallowing it", async () => {
    const adapter: Adapter = memoryAdapter();
    await adapter.subscribe("c", () => {
      throw new Error("listener bug");
    });
    await adapter.publish(
      "c",
      { event: "e", data: "1" },
      { history: 1, historyTtlMs: 1000, idempotencyTtlMs: 1000 },
    );
    const calls = vi.mocked(console.error).mock.calls;
    expect(calls).toHaveLength(1);
    expect(String(calls[0]?.[0])).toContain("memory adapter");
    vi.mocked(console.error).mockClear();
    await adapter.close();
  });
});

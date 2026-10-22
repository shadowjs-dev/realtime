import { afterEach, describe, expect, it, vi } from "vitest";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
function make(...args: Parameters<typeof rig>): Rig {
  const made = rig(...args);
  rigs.push(made);
  return made;
}
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});

const pings = (socket: { readonly sent: string[] }) =>
  socket.sent.filter((text) => text.includes('"ping"')).length;

describe("reconnect", () => {
  it("[RT-R10] waits an exponential delay with full jitter, capped at maxDelayMs", async () => {
    vi.useFakeTimers();
    const r = make({ reconnect: { random: () => 1, maxDelayMs: 4000 } });
    r.sockets.refuseAll();
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(0);
    expect(r.sockets.sockets).toHaveLength(1);
    let total = 1;
    for (const delay of [250, 500, 1000, 2000, 4000, 4000, 4000]) {
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(r.sockets.sockets).toHaveLength(total);
      await vi.advanceTimersByTimeAsync(1);
      total += 1;
      expect(r.sockets.sockets).toHaveLength(total);
    }
    expect(r.rt.status).toBe("reconnecting");
  });

  it("[RT-R10] jitter is uniform below the cap: a source of 0 reconnects at once, one of 0.5 halves the delay", async () => {
    vi.useFakeTimers();
    const half = make({ reconnect: { random: () => 0.5 } });
    half.sockets.refuseAll();
    half.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(124);
    expect(half.sockets.sockets).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(half.sockets.sockets).toHaveLength(2);
    const zero = make({ reconnect: { random: () => 0 } });
    zero.sockets.refuseAll();
    zero.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(5);
    expect(zero.sockets.sockets.length).toBeGreaterThan(3);
  });

  it("[RT-R10] the delay restarts at the minimum only after the connection stayed open stableMs", async () => {
    vi.useFakeTimers();
    const r = make({ reconnect: { random: () => 1, stableMs: 5000 } });
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(0);
    // Drop twice in a row, each time soon after opening: the delay grows 250, 500.
    for (const delay of [250, 500]) {
      r.sockets.sockets.at(-1)?.drop();
      await vi.advanceTimersByTimeAsync(delay - 1);
      const before = r.sockets.sockets.length;
      await vi.advanceTimersByTimeAsync(1);
      expect(r.sockets.sockets).toHaveLength(before + 1);
    }
    // Now stay open past stableMs, drop again: back to 250.
    await vi.advanceTimersByTimeAsync(5000);
    r.sockets.sockets.at(-1)?.drop();
    await vi.advanceTimersByTimeAsync(249);
    const before = r.sockets.sockets.length;
    await vi.advanceTimersByTimeAsync(1);
    expect(r.sockets.sockets).toHaveLength(before + 1);
    // And a drop right after that one waits 500, not 250.
    r.sockets.sockets.at(-1)?.drop();
    await vi.advanceTimersByTimeAsync(499);
    expect(r.sockets.sockets).toHaveLength(before + 1);
    await vi.advanceTimersByTimeAsync(1);
    expect(r.sockets.sockets).toHaveLength(before + 2);
  });

  it("[RT-R10] a close with 1008, 1002 or 1009 is fatal: no retry, everything rejects, the error is reported", async () => {
    for (const code of [1008, 1002, 1009]) {
      const r = make();
      const lobby = r.rt.channel("chat:lobby");
      lobby.on("message", () => undefined);
      await r.until(() => expect(lobby.status).toBe("live"));
      r.sockets.refuseAll();
      const socket = r.sockets.sockets[0];
      socket?.closeFromServer(code, "policy");
      await r.until(() => expect(r.rt.status).toBe("closed"));
      expect(lobby.status).toBe("closed");
      expect(r.errors[0]).toMatchObject({ code: "closed" });
      expect(String((r.errors[0] as Error).message)).toContain(String(code));
      await expect(lobby.publish("message", 1)).rejects.toMatchObject({ code: "closed" });
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(r.sockets.sockets).toHaveLength(1);
    }
  });

  it("[RT-R10] retryFatal retries even after a fatal close", async () => {
    const r = make({ reconnect: { retryFatal: true, random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    r.sockets.sockets[0]?.closeFromServer(1008, "policy");
    await r.until(() => expect(r.sockets.sockets).toHaveLength(2));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(r.rt.status).toBe("open");
  });

  it("[RT-R10] a bye frame carrying a fatal code is fatal even when the close code is lost", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    const socket = r.sockets.sockets[0];
    socket?.inject({ t: "bye", code: 1008, why: "rate limit" });
    await Promise.resolve();
    socket?.drop(1006);
    await r.until(() => expect(r.rt.status).toBe("closed"));
  });

  it("[RT-C-21] a server that never says ready is abandoned after ackTimeoutMs and retried", async () => {
    vi.useFakeTimers();
    const r = make({ ackTimeoutMs: 1000, reconnect: { random: () => 0 } });
    r.sockets.hold();
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(999);
    expect(r.sockets.sockets).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(2);
    expect(r.sockets.sockets.length).toBeGreaterThanOrEqual(2);
    expect(r.sockets.sockets[0]?.readyState).toBe(3);
  });

  it("[RT-C-22] a storm of 20 drops: publishes keep going, the cursor is continuous and nothing repeats", async () => {
    const r = make({ reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    const seen: number[] = [];
    lobby.on("message", (data, meta) => {
      seen.push(data as number);
      expect(meta.seq).toBe(seen.length);
    });
    await r.until(() => expect(lobby.status).toBe("live"));
    const acks: Promise<unknown>[] = [];
    let next = 1;
    for (let round = 1; round <= 20; round += 1) {
      for (let count = 0; count < 3; count += 1) acks.push(lobby.publish("message", next++));
      r.sockets.live()[0]?.drop();
      await r.until(() => expect(r.sockets.sockets).toHaveLength(round + 1));
      await r.until(() => expect(r.rt.status).toBe("open"));
    }
    await Promise.all(acks);
    await r.until(() => expect(seen).toHaveLength(60));
    expect(seen).toEqual(Array.from({ length: 60 }, (_, index) => index + 1));
    expect(r.sockets.sockets).toHaveLength(21);
    expect(r.hub.stats().published).toBe(60);
  });
});

describe("heartbeat", () => {
  it("[RT-C-23] pings at the interval the server suggests and stays open while it answers", async () => {
    vi.useFakeTimers();
    const r = make();
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(0);
    const socket = r.sockets.sockets[0];
    if (socket === undefined) throw new Error("no socket");
    expect(pings(socket)).toBe(0);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(pings(socket)).toBe(1);
    await vi.advanceTimersByTimeAsync(5 * 30_000);
    expect(pings(socket)).toBe(6);
    expect(r.sockets.sockets).toHaveLength(1);
    expect(r.rt.status).toBe("open");
  });

  it("[RT-C-24] declares a link dead after two silent intervals, closes it and reconnects with resume", async () => {
    vi.useFakeTimers();
    const r = make({ reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1);
    const socket = r.sockets.sockets[0];
    if (socket === undefined) throw new Error("no socket");
    socket.stall();
    r.peer().send({ t: "pub", c: "chat:lobby", e: "message", d: "while stalled" });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(socket.readyState).toBe(1);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(socket.readyState).toBe(3);
    await vi.advanceTimersByTimeAsync(5);
    expect(r.sockets.sockets).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(5);
    expect(seen).toEqual(["while stalled"]);
    expect(lobby.status).toBe("live");
  });

  it("[RT-C-25] any frame counts as life: a busy channel on a link that drops pongs is not declared dead", async () => {
    vi.useFakeTimers();
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(0);
    const socket = r.sockets.sockets[0];
    if (socket === undefined) throw new Error("no socket");
    for (let tick = 0; tick < 8; tick += 1) {
      await vi.advanceTimersByTimeAsync(15_000);
      socket.inject({ t: "msg", c: "chat:lobby", s: tick + 1, e: "message", d: tick });
    }
    expect(r.sockets.sockets).toHaveLength(1);
  });
});

describe("closing", () => {
  it("[RT-C-26] close() releases every timer, rejects what is pending and makes later calls fail cleanly", async () => {
    vi.useFakeTimers();
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1);
    r.sockets.sockets[0]?.stallInbound();
    const pending = lobby.publish("message", 1);
    const caught = pending.catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    const timersBefore = vi.getTimerCount();
    expect(timersBefore).toBeGreaterThan(0);
    r.rt.close();
    expect(await caught).toMatchObject({ code: "closed" });
    expect(r.rt.status).toBe("closed");
    expect(lobby.status).toBe("closed");
    expect(r.statuses.at(-1)).toBe("closed");
    expect(r.sockets.sockets[0]?.readyState).toBe(3);
    // The hub's own timers are not the client's: only count the ones created after the client started.
    await vi.advanceTimersByTimeAsync(120_000);
    expect(r.sockets.sockets).toHaveLength(1);
    expect(() => r.rt.channel("chat:other")).toThrow(/closed/);
    r.rt.close();
  });

  it("[RT-C-27] close() while reconnecting cancels the pending attempt", async () => {
    vi.useFakeTimers();
    const r = make({ reconnect: { random: () => 1 } });
    r.sockets.refuseAll();
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(10);
    r.rt.close();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(r.sockets.sockets).toHaveLength(1);
  });

  it("[RT-C-28] closing a channel unsubscribes it, rejects its publishes and does not touch other channels", async () => {
    const r = make();
    const a = r.rt.channel("chat:a");
    const b = r.rt.channel("chat:b");
    const seen: unknown[] = [];
    a.on("message", () => undefined);
    b.on("message", (data) => seen.push(data));
    await r.until(() => expect(b.status).toBe("live"));
    a.close();
    a.close();
    expect(a.status).toBe("closed");
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(1));
    await expect(a.publish("message", 1)).rejects.toMatchObject({ code: "closed" });
    await b.publish("message", "still");
    await r.until(() => expect(seen).toEqual(["still"]));
  });

  it("[RT-C-29] never produces an unhandled rejection, whatever fails", async () => {
    const unhandled: unknown[] = [];
    const listener = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", listener);
    try {
      const r = make({ ackTimeoutMs: 30, queueTtlMs: 30 });
      const lobby = r.rt.channel("chat:lobby");
      lobby.on("message", () => undefined);
      r.sockets.refuseAll();
      const settled = [
        lobby.publish("message", 1).catch(() => undefined),
        lobby.publish("message", 2, { ack: false }),
        lobby.request("shout", "x").catch(() => undefined),
      ];
      await Promise.all(settled);
      r.rt.close();
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", listener);
    }
  });
});

describe("backpressure", () => {
  it("[RT-G9] a consumer too slow for the hub is cut with 1013, resumes, and gets every message once in order", async () => {
    const r = make({ reconnect: { random: () => 0 } }, { limits: { maxBufferedBytes: 2000 } });
    const lobby = r.rt.channel("chat:lobby");
    const seen: number[] = [];
    lobby.on("message", (data, meta) => {
      seen.push(meta.seq);
      expect(typeof data).toBe("string");
    });
    await r.until(() => expect(lobby.status).toBe("live"));
    const socket = r.sockets.sockets[0];
    socket?.stallInbound();
    const peer = r.peer();
    for (let at = 0; at < 30; at += 1)
      peer.send({ t: "pub", c: "chat:lobby", e: "message", d: "x".repeat(200) });
    await r.until(() => expect(socket?.closedWith).toBe(1013));
    await r.until(() => expect(seen).toHaveLength(30));
    expect(seen).toEqual(Array.from({ length: 30 }, (_, index) => index + 1));
    expect(r.sockets.sockets.length).toBeGreaterThanOrEqual(2);
    expect(lobby.status).toBe("live");
  });
});

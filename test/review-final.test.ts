import { afterEach, describe, expect, it, vi } from "vitest";
import { openStream } from "../src/client/sse";
import type { ServerFrame } from "../src/protocol";
import { createHub, type Hub } from "../src/server/index";
import { connectTest } from "../src/testing";

const hubs: Hub[] = [];
afterEach(async () => {
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
});

const eventOf = (frame: Record<string, unknown>): string =>
  `event: rt\ndata: ${JSON.stringify(frame)}\n\n`;

describe("the event stream accepts the frames its hub is configured to send", () => {
  it("[RT-FINAL-1] a frame larger than the default cap arrives when the hub announced a larger maxMessageBytes", async () => {
    const big = "x".repeat(1_500_000);
    const text =
      eventOf({ t: "ready", v: 1, id: "c1", hb: 0, max: 4_000_000 }) +
      eventOf({ t: "msg", c: "room:a", s: 1, e: "big", d: big });
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        const bytes = new TextEncoder().encode(text);
        // Chunks of 64 KiB, like a network read.
        for (let at = 0; at < bytes.length; at += 65_536)
          controller.enqueue(bytes.slice(at, at + 65_536));
        controller.close();
      },
    });
    const frames: ServerFrame[] = [];
    const closed: unknown[] = [];
    openStream(
      "http://localhost/rt",
      async () => new Response(body, { status: 200 }),
      () => undefined,
      {
        rx: () => undefined,
        frame: (frame) => frames.push(frame),
        closed: (info) => closed.push(info),
      },
    );
    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(frames.map((frame) => frame.t)).toEqual(["ready", "msg"]);
  });

  it("[RT-FINAL-1] a hub that announces an absurd frame size does not lift the cap without bound", async () => {
    const head = new TextEncoder().encode(
      eventOf({ t: "ready", v: 1, id: "c1", hb: 0, max: Number.MAX_SAFE_INTEGER }),
    );
    const chunk = new TextEncoder().encode("x".repeat(65_536));
    let pulled = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled === 0) controller.enqueue(head);
        else controller.enqueue(chunk);
        pulled += 1;
        if (pulled > 20_000) controller.close();
      },
    });
    const closed: unknown[] = [];
    openStream(
      "http://localhost/rt",
      async () => new Response(body, { status: 200 }),
      () => undefined,
      { rx: () => undefined, frame: () => undefined, closed: (info) => closed.push(info) },
    );
    await vi.waitFor(() => expect(closed).toHaveLength(1));
    // 16 MiB of the endless line is 256 chunks; the reader may have a few more queued.
    expect(pulled).toBeLessThan(600);
  });
});

describe("the subscription bucket never leaves a subscription the client dropped", () => {
  it("[RT-FINAL-2] an unsubscribe refused for lack of tokens would keep the subscription; it is applied, so a later subscribe is not a conflict", async () => {
    let at = 1_000;
    const hub = createHub({
      channels: { "p:{x}": { subscribe: true } },
      onError: () => undefined,
      now: () => at,
    });
    hubs.push(hub);
    const client = connectTest(hub, { user: "ann" });
    client.send({ t: "sub", n: 1, c: "p:keep" });
    // Churn on another channel until the bucket is empty.
    for (let step = 0; step < 100; step += 1)
      client.send(
        step % 2 === 0 ? { t: "sub", n: 100 + step, c: "p:other" } : { t: "unsub", c: "p:other" },
      );
    client.send({ t: "unsub", c: "p:keep" });
    await client.settled();
    at += 10_000;
    client.send({ t: "sub", n: 500, c: "p:keep" });
    await client.settled();
    const answer = client.frames.find((frame) => frame.t === "subbed" && frame.n === 500);
    const conflicts = client.frames.filter(
      (frame) => frame.t === "err" && frame.code === "conflict",
    );
    expect(conflicts).toEqual([]);
    expect(answer).toBeDefined();
  });
});

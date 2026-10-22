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

const declared = (
  url: URL | undefined,
): { c: string; after?: number; echo?: boolean; presence?: unknown; watch?: boolean }[] =>
  JSON.parse(url?.searchParams.get("s") ?? "[]");

describe("the server-sent events transport", () => {
  it("[RT-R11] declares the channels in the stream request, batched in one tick, and never opens a socket", async () => {
    const r = make({ transport: "sse" });
    r.rt.channel("chat:a").on("message", () => undefined);
    r.rt.channel("chat:b", { echo: false }).on("message", () => undefined);
    await r.until(() => expect(r.rt.channel("chat:b").status).toBe("live"));
    expect(r.fetches.streams).toHaveLength(1);
    expect(r.sockets.sockets).toHaveLength(0);
    expect(declared(r.fetches.streams[0])).toEqual([{ c: "chat:a" }, { c: "chat:b", echo: false }]);
    expect(r.rt.status).toBe("open");
    expect(r.rt.connectionId).toBeDefined();
  });

  it("[RT-R11] changing the set reopens the stream with the current cursor of each channel", async () => {
    const r = make({ transport: "sse" });
    const a = r.rt.channel("chat:a");
    const seen: unknown[] = [];
    a.on("message", (data) => seen.push(data));
    await r.until(() => expect(a.status).toBe("live"));
    await a.publish("message", 1);
    await a.publish("message", 2);
    await r.until(() => expect(seen).toEqual([1, 2]));
    const b = r.rt.channel("chat:b");
    b.on("message", () => undefined);
    await r.until(() => expect(b.status).toBe("live"));
    expect(r.fetches.streams).toHaveLength(2);
    expect(declared(r.fetches.streams[1])).toEqual([{ c: "chat:a", after: 2 }, { c: "chat:b" }]);
    expect(seen).toEqual([1, 2]);
    expect(a.status).toBe("live");
    // A message published meanwhile on a is delivered once, after the reopen.
    await a.publish("message", 3);
    await r.until(() => expect(seen).toEqual([1, 2, 3]));
  });

  it("[RT-R11] publishes by POST to <url>/send with the connection id, and resolves with the acknowledgment", async () => {
    const r = make({ transport: "sse", url: "http://localhost/rt?token=7" });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    const ack = await lobby.publish("message", { n: 1 });
    expect(ack).toEqual({ seq: 1, duplicate: false });
    const post = r.fetches.posts[0];
    expect(post?.url.pathname).toBe("/rt/send");
    expect(post?.url.search).toBe("?token=7");
    expect(post?.body.cid).toBe(r.rt.connectionId);
    expect(post?.body.frames[0]).toMatchObject({
      t: "pub",
      c: "chat:lobby",
      e: "message",
      d: { n: 1 },
    });
  });

  it("[RT-R11] a custom postUrl is used, and headers (also from a function) go on the stream and the POSTs", async () => {
    const r = make({
      transport: "sse",
      postUrl: "http://localhost/elsewhere",
      headers: async () => ({ authorization: "Bearer t" }),
    });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await lobby.publish("message", 1);
    expect(r.fetches.posts[0]?.url.pathname).toBe("/elsewhere");
    expect(r.fetches.headers.length).toBeGreaterThanOrEqual(2);
    for (const headers of r.fetches.headers) expect(headers.get("authorization")).toBe("Bearer t");
  });

  it("[RT-R11] publishes made together travel in one POST, in order", async () => {
    const r = make({ transport: "sse" }, { limits: { publishBurst: 100 } });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", 0);
    const acks = await Promise.all([1, 2, 3, 4, 5].map((n) => lobby.publish("message", n)));
    expect(acks.map((ack) => ack.seq)).toEqual([2, 3, 4, 5, 6]);
    const sizes = r.fetches.posts.map((post) => post.body.frames.length);
    expect(sizes).toEqual([1, 1, 4]);
  });

  it("[RT-R11] a request goes by POST without a subscription and resolves with the reply", async () => {
    const r = make({ transport: "sse" });
    const lobby = r.rt.channel("chat:lobby");
    expect(await lobby.request("shout", "hey")).toBe("HEY");
    expect(r.fetches.streams).toHaveLength(1);
    expect(declared(r.fetches.streams[0])).toEqual([]);
    await expect(lobby.request("nothing", 1)).rejects.toMatchObject({ code: "no_handler" });
  });

  it("[RT-R11] resumes after the stream drops: reconnects with the cursor and loses nothing", async () => {
    const r = make({ transport: "sse", reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    const seen: [unknown, boolean][] = [];
    lobby.on("message", (data, meta) => seen.push([data, meta.replayed]));
    await r.until(() => expect(lobby.status).toBe("live"));
    await lobby.publish("message", 1);
    await r.until(() => expect(seen).toHaveLength(1));
    r.fetches.dropStreams();
    r.peer().send({ t: "pub", c: "chat:lobby", e: "message", d: 2 });
    await r.until(() => expect(seen).toHaveLength(2));
    expect(seen).toEqual([
      [1, false],
      [2, true],
    ]);
    expect(declared(r.fetches.streams[1])).toEqual([{ c: "chat:lobby", after: 1 }]);
  });

  it("[RT-R9] a POST that fails on the network or with 503 is retried with the same key and applied once", async () => {
    const r = make({ transport: "sse", reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    r.fetches.failPosts(1);
    const first = await lobby.publish("message", "a");
    r.fetches.answerPosts(1, 503);
    const second = await lobby.publish("message", "b");
    expect([first.seq, second.seq]).toEqual([1, 2]);
    expect(r.hub.stats().published).toBe(2);
    const keysOf = (value: string): string[] =>
      r.fetches.posts
        .flatMap((post) => post.body.frames)
        .filter((frame) => frame["d"] === value)
        .map((frame) => String(frame["k"]));
    expect(keysOf("a")).toHaveLength(2);
    expect(new Set(keysOf("a")).size).toBe(1);
    expect(new Set(keysOf("b")).size).toBe(1);
  });

  it("[RT-C-36] a POST the server refuses (403) rejects the publishes of that request with the code", async () => {
    const r = make({ transport: "sse" });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await r.until(() => expect(lobby.status).toBe("live"));
    r.fetches.answerPosts(1, 403, "forbidden");
    await expect(lobby.publish("message", 1)).rejects.toMatchObject({ code: "forbidden" });
    await expect(lobby.publish("message", 2)).resolves.toMatchObject({ seq: 1 });
  });

  it("[RT-C-37] a request whose POST fails on the network rejects with unavailable instead of repeating", async () => {
    const r = make({ transport: "sse", reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    await lobby.publish("message", 0);
    r.fetches.failPosts(1);
    await expect(lobby.request("shout", "x")).rejects.toMatchObject({ code: "unavailable" });
    expect(await lobby.request("shout", "y")).toBe("Y");
  });

  it("[RT-C-38] a stream refused with 401 is fatal; one refused with 503 is retried", async () => {
    const refused = make({ transport: "sse" });
    refused.fetches.refuseStreams(401);
    refused.rt.channel("chat:lobby").on("message", () => undefined);
    await refused.until(() => expect(refused.rt.status).toBe("closed"));
    expect(refused.errors[0]).toMatchObject({ code: "closed" });
    expect(refused.fetches.streams).toHaveLength(0);

    const later = make({ transport: "sse", reconnect: { random: () => 0 } });
    later.fetches.refuseStreams(503);
    const lobby = later.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    await later.until(() => expect(later.rt.status).toBe("reconnecting"));
    later.fetches.refuseStreams(0);
    await later.until(() => expect(lobby.status).toBe("live"));
  });

  it("[RT-C-39] more channels than one stream carries: the extra ones close with limit", async () => {
    const r = make({ transport: "sse" });
    const channels = Array.from({ length: 66 }, (_, index) => r.rt.channel(`chat:${index}`));
    for (const channel of channels) channel.on("message", () => undefined);
    await r.until(() => expect(channels[0]?.status).toBe("live"));
    expect(channels.filter((channel) => channel.status === "closed")).toHaveLength(2);
    expect(r.errors).toHaveLength(2);
    expect(r.errors[0]).toMatchObject({ code: "limit" });
  });

  it("[RT-R12] presence: joins from the stream request and changes by POST for a signed-in user", async () => {
    const r = make({ transport: "sse" }, {}, { user: "ann" });
    const watcher = r.another({ transport: "ws" }).channel("chat:lobby");
    watcher.presence.on(() => undefined);
    const joiner = r.rt.channel("chat:lobby", { presence: { n: 1 } });
    joiner.on("message", () => undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    expect(declared(r.fetches.streams[0])).toEqual([{ c: "chat:lobby", presence: { n: 1 } }]);
    joiner.presence.set({ n: 2 });
    await r.until(() => expect(watcher.presence.members()[0]?.s).toEqual({ n: 2 }));
    expect(r.fetches.posts.at(-1)?.body.frames[0]).toEqual({
      t: "pres",
      c: "chat:lobby",
      s: { n: 2 },
    });
    expect(watcher.presence.members()[0]?.u).toBe("ann");
    joiner.presence.leave();
    await r.until(() => expect(watcher.presence.members()).toHaveLength(0));
  });

  it("[RT-R12] an anonymous user's presence change is refused and reported", async () => {
    const r = make({ transport: "sse" });
    const joiner = r.rt.channel("chat:lobby", { presence: 1 });
    joiner.on("message", () => undefined);
    await r.until(() => expect(joiner.status).toBe("live"));
    await r.until(() => expect(joiner.presence.members()).toHaveLength(1));
    joiner.presence.set(2);
    await r.until(() =>
      expect(r.errors.some((error) => (error as { code?: string }).code === "unauthorized")).toBe(
        true,
      ),
    );
  });
});

describe("falling back from WebSocket", () => {
  it("[RT-R11] two failed upgrades in a row switch to server-sent events for good", async () => {
    const r = make({ transport: "auto", reconnect: { random: () => 0 } });
    r.sockets.refuseAll();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(r.sockets.sockets).toHaveLength(2);
    expect(r.fetches.streams).toHaveLength(1);
    await lobby.publish("message", "over sse");
    await r.until(() => expect(seen).toEqual(["over sse"]));
    r.sockets.allow();
    r.fetches.dropStreams();
    await r.until(() => expect(r.fetches.streams).toHaveLength(2));
    expect(r.sockets.sockets).toHaveLength(2);
  });

  it("[RT-R11] one failed upgrade followed by a success does not fall back, and the count restarts after a success", async () => {
    const r = make({ transport: "auto", reconnect: { random: () => 0 } });
    const lobby = r.rt.channel("chat:lobby");
    lobby.on("message", () => undefined);
    r.sockets.refuseNext(1);
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(r.fetches.streams).toHaveLength(0);
    r.sockets.refuseNext(1);
    r.sockets.live()[0]?.drop();
    await r.until(() => expect(r.sockets.sockets).toHaveLength(4));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(r.fetches.streams).toHaveLength(0);
  });

  it("[RT-R11] transport ws never falls back", async () => {
    const r = make({ transport: "ws", reconnect: { random: () => 0 } });
    r.sockets.refuseAll();
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await r.until(() => expect(r.sockets.sockets.length).toBeGreaterThan(4));
    expect(r.fetches.streams).toHaveLength(0);
  });

  it("[RT-R11] publishes in flight on the failed socket path are not lost by the switch", async () => {
    const r = make({ transport: "auto", reconnect: { random: () => 0 } });
    r.sockets.refuseAll();
    const ack = await r.rt.channel("chat:lobby").publish("message", "x");
    expect(ack.seq).toBe(1);
    expect(r.fetches.streams).toHaveLength(1);
  });
});

describe("liveness over server-sent events", () => {
  it("[RT-C-40] a stream that goes silent for two intervals is dropped and reopened with the cursor", async () => {
    vi.useFakeTimers();
    const r = make({ transport: "sse", reconnect: { random: () => 0 } });
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await vi.advanceTimersByTimeAsync(5);
    expect(r.fetches.streams).toHaveLength(1);
    // The in-memory stream sends nothing by itself: no keepalive comment arrives.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(r.fetches.streams).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(30_000);
    await vi.advanceTimersByTimeAsync(5);
    expect(r.fetches.streams).toHaveLength(2);
  });
});

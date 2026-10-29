import { afterEach, describe, expect, it } from "vitest";
import type { ServerFrame } from "../src/protocol";
import { type Adapter, createHub, type Hub, memoryAdapter } from "../src/server/index";
import { connectTest } from "../src/testing";
import { type Rig, rig } from "./fixtures/harness";

const hubs: Hub[] = [];
const rigs: Rig[] = [];
afterEach(async () => {
  await Promise.all(hubs.splice(0).map((hub) => hub.close()));
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});
const quiet = (): void => undefined;
const rules = { "p:{x}": { subscribe: true, presence: true, publish: true } };

const acks = (frames: readonly ServerFrame[]) =>
  frames.flatMap((frame) => (frame.t === "ack" ? [frame] : []));
const errorCodes = (frames: readonly ServerFrame[]): string[] =>
  frames.flatMap((frame) => (frame.t === "err" ? [frame.code] : []));

describe("idempotency keys belong to their publisher", () => {
  it("[RT-R7-1] another user reusing a key is not a duplicate, and cannot suppress the first publish", async () => {
    const hub = createHub({ channels: rules, onError: quiet });
    hubs.push(hub);
    const ann = connectTest(hub, { user: "ann" });
    const bob = connectTest(hub, { user: "bob" });
    ann.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    await ann.settled();
    bob.send({ t: "pub", c: "p:1", e: "m", d: "2", n: 1, k: "same" });
    await bob.settled();
    expect(acks(ann.frames)).toEqual([{ t: "ack", n: 1, s: 1 }]);
    expect(acks(bob.frames)).toEqual([{ t: "ack", n: 1, s: 2 }]);
  });

  it("[RT-R7-1] a key the server used is not a duplicate for a client, and the other way around", async () => {
    const hub = createHub({ channels: rules, onError: quiet });
    hubs.push(hub);
    const ann = connectTest(hub, { user: "ann" });
    await hub.publish("p:1", "m", "server", { key: "same" });
    ann.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    await ann.settled();
    expect(acks(ann.frames)).toEqual([{ t: "ack", n: 1, s: 2 }]);
    const again = await hub.publish("p:1", "m", "server", { key: "same" });
    expect(again).toEqual({ seq: 1, duplicate: true });
  });

  it("[RT-R7-1] the same user on a new connection still resends a key as a duplicate", async () => {
    const hub = createHub({ channels: rules, onError: quiet });
    hubs.push(hub);
    const first = connectTest(hub, { user: "ann" });
    first.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    await first.settled();
    const second = connectTest(hub, { user: "ann" });
    second.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    await second.settled();
    expect(acks(second.frames)).toEqual([{ t: "ack", n: 1, s: 1, dup: true }]);
  });

  it("[RT-R7-1] anonymous connections share one scope, so a resend on a new connection is still a duplicate; a user's key is not theirs", async () => {
    const hub = createHub({ channels: rules, onError: quiet });
    hubs.push(hub);
    const one = connectTest(hub);
    one.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    await one.settled();
    const two = connectTest(hub);
    two.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "same" });
    const ann = connectTest(hub, { user: "ann" });
    ann.send({ t: "pub", c: "p:1", e: "m", d: "2", n: 1, k: "same" });
    await two.settled();
    await ann.settled();
    expect(acks(two.frames)).toEqual([{ t: "ack", n: 1, s: 1, dup: true }]);
    expect(acks(ann.frames)).toEqual([{ t: "ack", n: 1, s: 2 }]);
  });

  it("[RT-R7-1] a user id that looks like a key suffix cannot collide with another scope", async () => {
    const hub = createHub({ channels: rules, onError: quiet });
    hubs.push(hub);
    const a = connectTest(hub, { user: "x:y" });
    const b = connectTest(hub, { user: "x" });
    a.send({ t: "pub", c: "p:1", e: "m", d: "1", n: 1, k: "k" });
    b.send({ t: "pub", c: "p:1", e: "m", d: "2", n: 1, k: "y:k" });
    await a.settled();
    await b.settled();
    expect(acks(a.frames)).toEqual([{ t: "ack", n: 1, s: 1 }]);
    expect(acks(b.frames)).toEqual([{ t: "ack", n: 1, s: 2 }]);
  });
});

describe("presence capacity under concurrent joins", () => {
  it("[RT-R7-2] ten joins in one tick with a cap of two leave two members and refuse the rest", async () => {
    const hub = createHub({
      adapter: memoryAdapter(),
      channels: rules,
      onError: quiet,
      limits: { maxPresenceMembers: 2 },
    });
    hubs.push(hub);
    const clients = Array.from({ length: 10 }, (_, at) => connectTest(hub, { user: `u${at}` }));
    for (const client of clients)
      client.send({ t: "sub", n: 1, c: "p:1", presence: { here: true } });
    await Promise.all(clients.map((client) => client.settled()));
    const refused = clients.filter((client) => errorCodes(client.frames).includes("limit"));
    expect(refused).toHaveLength(8);
    const probe = connectTest(hub, { user: "probe" });
    probe.send({ t: "sub", n: 1, c: "p:1", watch: true });
    await probe.settled();
    const sync = probe.frames.find((frame) => frame.t === "pres" && frame.op === "sync");
    expect(sync?.t === "pres" && sync.op === "sync" ? sync.m : undefined).toHaveLength(2);
  });
});

describe("a revoke reaches a subscribe that is still being judged", () => {
  const slowHub = (): { hub: Hub; open: () => void } => {
    let open: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      open = resolve;
    });
    const hub = createHub({
      channels: {
        "p:{x}": {
          subscribe: async () => {
            await gate;
            return true;
          },
        },
      },
      onError: quiet,
    });
    hubs.push(hub);
    return { hub, open };
  };

  it("[RT-R7-3] a revoke for the user while the rule is awaited refuses the subscribe", async () => {
    const { hub, open } = slowHub();
    const ann = connectTest(hub, { user: "ann" });
    ann.send({ t: "sub", n: 1, c: "p:1" });
    await Promise.resolve();
    expect(hub.revoke({ user: "ann" })).toBe(1);
    open();
    await ann.settled();
    expect(ann.frames.some((frame) => frame.t === "subbed")).toBe(false);
    expect(errorCodes(ann.frames)).toEqual(["forbidden"]);
    expect(hub.stats().subscriptions).toBe(0);
  });

  it("[RT-R7-3] a revoke for the connection or the channel does the same, one for someone else does not", async () => {
    const { hub, open } = slowHub();
    const ann = connectTest(hub, { user: "ann" });
    const bob = connectTest(hub, { user: "bob" });
    ann.send({ t: "sub", n: 1, c: "p:1" });
    ann.send({ t: "sub", n: 2, c: "p:2" });
    bob.send({ t: "sub", n: 1, c: "p:1" });
    await Promise.resolve();
    hub.revoke({ user: "carol" });
    hub.revoke({ connection: ann.id, channel: "p:1" });
    open();
    await ann.settled();
    await bob.settled();
    expect(ann.frames.filter((frame) => frame.t === "subbed")).toHaveLength(1);
    expect(bob.frames.filter((frame) => frame.t === "subbed")).toHaveLength(1);
    expect(errorCodes(ann.frames)).toEqual(["forbidden"]);
  });

  it("[RT-R7-3] a subscribe made after the revoke is not affected", async () => {
    const { hub, open } = slowHub();
    open();
    const ann = connectTest(hub, { user: "ann" });
    hub.revoke({ user: "ann" });
    ann.send({ t: "sub", n: 1, c: "p:1" });
    await ann.settled();
    expect(ann.frames.some((frame) => frame.t === "subbed")).toBe(true);
  });
});

describe("a reported gap is reported once", () => {
  it("[RT-R7-4] a client whose missed history expired does not see the same gap on every reconnect", async () => {
    const base = memoryAdapter();
    let expired = false;
    const adapter: Adapter = {
      ...base,
      subscribe: (channel, listener) => base.subscribe(channel, listener),
      history: async (channel, after, limit) => {
        const page = await base.history(channel, after, limit);
        return expired && after !== Number.MAX_SAFE_INTEGER
          ? { ...page, messages: [], oldest: 0 }
          : expired
            ? { ...page, oldest: 0 }
            : page;
      },
    };
    const r = rig({}, { adapter });
    rigs.push(r);
    const lobby = r.rt.channel("chat:lobby");
    const gaps: unknown[] = [];
    lobby.on("message", () => undefined);
    lobby.onGap((info) => gaps.push(info));
    await r.until(() => expect(lobby.status).toBe("live"));
    r.sockets.sockets[0]?.drop();
    const peer = r.peer();
    for (let at = 1; at <= 3; at += 1)
      peer.send({ t: "pub", c: "chat:lobby", e: "message", d: at });
    await peer.settled();
    expired = true;
    await r.until(() => expect(gaps).toHaveLength(1));
    await r.until(() => expect(lobby.status).toBe("live"));
    r.sockets.sockets[1]?.drop();
    await r.until(() => expect(r.sockets.sockets.length).toBeGreaterThanOrEqual(3));
    await r.until(() => expect(lobby.status).toBe("live"));
    expect(gaps).toEqual([{ after: 0, head: 3, reset: false }]);
  });
});

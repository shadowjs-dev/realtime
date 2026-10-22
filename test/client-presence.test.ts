import { afterEach, describe, expect, it } from "vitest";
import { applyPresence } from "../src/client/presence";
import type { Member } from "../src/index";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
function make(...args: Parameters<typeof rig>): Rig {
  const made = rig(...args);
  rigs.push(made);
  return made;
}
afterEach(async () => {
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});

const member = (id: string, v: number, s: unknown = null): Member => ({ id, v, s: s as never });

describe("the presence reducer", () => {
  it("[RT-G11] a sync replaces the table; join and set apply by version; a repeated or older event changes nothing", () => {
    const table = new Map<string, Member>();
    expect(applyPresence(table, "sync", [member("a", 1), member("b", 1)])).toBe(true);
    expect([...table.keys()]).toEqual(["a", "b"]);
    expect(applyPresence(table, "set", member("a", 2, "x"))).toBe(true);
    expect(applyPresence(table, "set", member("a", 2, "y"))).toBe(false);
    expect(applyPresence(table, "set", member("a", 1, "old"))).toBe(false);
    expect(table.get("a")?.s).toBe("x");
    expect([...table.keys()]).toEqual(["a", "b"]);
    expect(applyPresence(table, "join", member("c", 1))).toBe(true);
    expect(applyPresence(table, "join", member("c", 1))).toBe(false);
    expect(applyPresence(table, "leave", member("b", 1))).toBe(true);
    expect(applyPresence(table, "leave", member("b", 1))).toBe(false);
    expect(applyPresence(table, "leave", member("a", 1))).toBe(false);
    expect([...table.keys()]).toEqual(["a", "c"]);
    expect(applyPresence(table, "sync", [])).toBe(true);
    expect(table.size).toBe(0);
  });
});

describe("presence through the hub", () => {
  it("[RT-C-30] a joined member is seen by a watcher, with set and leave, ordered and without duplicates", async () => {
    const r = make();
    const other = r.another();
    const joiner = r.rt.channel("chat:lobby", { presence: { name: "ann" } });
    const watcher = other.channel("chat:lobby");
    const lists: Member[][] = [];
    watcher.presence.on((members) => lists.push([...members]));
    joiner.on("message", () => undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    const first = watcher.presence.members()[0];
    expect(first).toMatchObject({ id: r.rt.connectionId, s: { name: "ann" }, v: 1 });
    expect(joiner.presence.members()).toHaveLength(1);
    joiner.presence.set({ name: "ann", typing: true } as never);
    await r.until(() => expect(watcher.presence.members()[0]?.v).toBe(2));
    expect(watcher.presence.members()[0]?.s).toEqual({ name: "ann", typing: true });
    expect(joiner.presence.members()[0]?.v).toBe(2);
    joiner.presence.leave();
    await r.until(() => expect(watcher.presence.members()).toHaveLength(0));
    expect(joiner.status).toBe("live");
    expect(lists.map((list) => list.length)).toEqual([0, 1, 1, 0]);
  });

  it("[RT-C-31] members() is the same array until something changes, and keeps join order", async () => {
    const r = make();
    const a = r.rt.channel("chat:lobby", { presence: "a" });
    const watcher = r.another().channel("chat:lobby", { watch: true });
    watcher.on("message", () => undefined);
    a.on("message", () => undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    const b = r.another().channel("chat:lobby", { presence: "b" });
    await r.until(() => expect(watcher.presence.members()).toHaveLength(2));
    const list = watcher.presence.members();
    expect(watcher.presence.members()).toBe(list);
    expect(list.map((item) => item.s)).toEqual(["a", "b"]);
    b.presence.set("b2");
    await r.until(() => expect(watcher.presence.members()[1]?.s).toBe("b2"));
    expect(watcher.presence.members().map((item) => item.s)).toEqual(["a", "b2"]);
    expect(watcher.presence.members()).not.toBe(list);
  });

  it("[RT-C-32] joining after the channel subscribed switches the subscription without losing a message", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    const seen: unknown[] = [];
    lobby.on("message", (data) => seen.push(data));
    await r.until(() => expect(lobby.status).toBe("live"));
    const peer = r.peer();
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 1 });
    await r.until(() => expect(seen).toEqual([1]));
    lobby.presence.set("here");
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 2 });
    await r.until(() => expect(lobby.presence.members()).toHaveLength(1));
    peer.send({ t: "pub", c: "chat:lobby", e: "message", d: 3 });
    await r.until(() => expect(seen).toEqual([1, 2, 3]));
    expect(lobby.status).toBe("live");
  });

  it("[RT-C-33] after a reconnect the member list is rebuilt and this client appears once with its new id", async () => {
    const r = make({ reconnect: { random: () => 0 } });
    const watcher = r.another().channel("chat:lobby");
    const stop = watcher.presence.on(() => undefined);
    const joiner = r.rt.channel("chat:lobby", { presence: "ann" });
    joiner.on("message", () => undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    const before = r.rt.connectionId;
    r.sockets.sockets.find((socket) => socket.connection?.id === before)?.drop();
    await r.until(() => expect(r.rt.connectionId).not.toBe(before));
    await r.until(() =>
      expect(watcher.presence.members().map((item) => item.id)).toEqual([r.rt.connectionId]),
    );
    expect(joiner.presence.members().map((item) => item.id)).toEqual([r.rt.connectionId]);
    stop();
  });

  it("[RT-C-34] a state set while offline is what joins on the next connection", async () => {
    const r = make({ reconnect: { random: () => 0 } });
    const watcher = r.another().channel("chat:lobby");
    watcher.presence.on(() => undefined);
    const joiner = r.rt.channel("chat:lobby", { presence: "first" });
    joiner.on("message", () => undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    r.sockets.refuseNext(1);
    r.sockets.sockets.find((socket) => socket.connection?.id === r.rt.connectionId)?.drop();
    joiner.presence.set("second");
    await r.until(() =>
      expect(watcher.presence.members().map((item) => item.s)).toEqual(["second"]),
    );
  });

  it("[RT-C-35] presence the rule forbids closes the channel with the refusal", async () => {
    const r = make({}, { channels: { "nopres:{id}": { subscribe: true } } });
    const channel = r.rt.channel("nopres:1", { presence: "x" });
    await r.until(() => expect(channel.status).toBe("closed"));
    expect(r.errors[0]).toMatchObject({ code: "unauthorized" });
  });
});

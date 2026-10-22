import { createRoot } from "@shadow-js/core";
import { afterEach, describe, expect, it } from "vitest";
import { useChannel, useChannelEvent, useChannelPresence, useRealtimeStatus } from "../src/hooks";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
afterEach(async () => {
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});

describe("the hooks during the server render", () => {
  it("[RT-C-48] never connect, listen or create a channel, and answer with inert values", async () => {
    const r = rig();
    rigs.push(r);
    const result = createRoot((dispose) => {
      const handle = useChannel(r.rt, "chat:lobby", { presence: "x" as never });
      useChannelEvent(handle.channel, "message", () => undefined);
      const [members, setPresence] = useChannelPresence(handle.channel);
      setPresence("y" as never);
      const status = useRealtimeStatus(r.rt);
      const out = { handle, members: members(), status: status() };
      dispose();
      return out;
    });
    await Promise.resolve();
    expect(result.handle.status()).toBe("idle");
    expect(result.handle.lastMessage()).toBeUndefined();
    expect(result.members).toEqual([]);
    expect(result.status).toBe("connecting");
    expect(result.handle.channel.name).toBe("chat:lobby");
    expect(result.handle.channel.on("x", () => undefined)).toBeTypeOf("function");
    await expect(result.handle.publish("message", 1)).rejects.toMatchObject({ code: "closed" });
    await expect(result.handle.request("shout", "x")).rejects.toMatchObject({ code: "closed" });
    expect(r.sockets.sockets).toHaveLength(0);
    expect(r.fetches.streams).toHaveLength(0);
    expect(r.hub.stats().connections).toBe(0);
  });
});

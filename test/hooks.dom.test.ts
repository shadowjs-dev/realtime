import { flush, renderHook } from "@shadow-js/testing";
import { afterEach, describe, expect, it } from "vitest";
import { createRealtime } from "../src/client/index";
import { useChannel, useChannelEvent, useChannelPresence, useRealtimeStatus } from "../src/hooks";
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

describe("useChannel", () => {
  it("[RT-C-41] status and lastMessage are signals, publish works, and disposing unsubscribes", async () => {
    const r = make();
    const view = renderHook(() => useChannel(r.rt, "chat:lobby"));
    const lobby = view.result.current;
    expect(lobby.status()).toBe("subscribing");
    await r.until(() => expect(lobby.status()).toBe("live"));
    expect(lobby.lastMessage()).toBeUndefined();
    const ack = await lobby.publish("message", { text: "hi" });
    expect(ack.seq).toBe(1);
    await r.until(() =>
      expect(lobby.lastMessage()).toMatchObject({ event: "message", data: { text: "hi" } }),
    );
    view.dispose();
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(0));
    expect(lobby.channel.status).toBe("idle");
    await flush();
  });

  it("[RT-C-42] components share one channel; disposing one keeps the subscription for the other", async () => {
    const r = make();
    const first = renderHook(() => useChannel(r.rt, "chat:lobby"));
    const second = renderHook(() => useChannel(r.rt, "chat:lobby"));
    expect(second.result.current.channel).toBe(first.result.current.channel);
    await r.until(() => expect(second.result.current.status()).toBe("live"));
    first.dispose();
    await flush();
    expect(r.hub.stats().subscriptions).toBe(1);
    r.peer().send({ t: "pub", c: "chat:lobby", e: "message", d: "x" });
    await r.until(() => expect(second.result.current.lastMessage()?.data).toBe("x"));
    second.dispose();
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(0));
  });

  it("[RT-C-43] request resolves with the reply", async () => {
    const r = make();
    const view = renderHook(() => useChannel(r.rt, "chat:lobby"));
    expect(await view.result.current.request("shout", "hey")).toBe("HEY");
    view.dispose();
  });

  it("[RT-C-44] mount and unmount 300 times leaves no listener, no subscription and no pending work", async () => {
    const r = make();
    const lobby = r.rt.channel("chat:lobby");
    for (let index = 0; index < 300; index += 1) {
      const view = renderHook(() => {
        const handle = useChannel(r.rt, "chat:lobby");
        useChannelEvent(handle.channel, "message", () => undefined);
        useChannelPresence(handle.channel);
        return handle;
      });
      view.dispose();
    }
    await r.until(() => expect(r.hub.stats().subscriptions).toBe(0));
    await r.until(() => expect(lobby.status).toBe("idle"));
    expect(r.errors).toEqual([]);
  });
});

describe("useChannelEvent", () => {
  it("[RT-C-45] calls the handler for the event while the component lives, then stops", async () => {
    const r = make();
    const seen: unknown[] = [];
    const view = renderHook(() => {
      const { channel } = useChannel(r.rt, "chat:lobby");
      useChannelEvent(channel, "message", (data) => seen.push(data));
      return channel;
    });
    await r.until(() => expect(view.result.current.status).toBe("live"));
    await view.result.current.publish("message", 1);
    await r.until(() => expect(seen).toEqual([1]));
    view.dispose();
    await view.result.current.publish("message", 2);
    await flush();
    expect(seen).toEqual([1]);
  });
});

describe("useChannelPresence", () => {
  it("[RT-C-46] members is a signal, setPresence joins and changes the state, undefined leaves, dispose leaves", async () => {
    const r = make();
    const watcher = r.another().channel("chat:lobby");
    watcher.presence.on(() => undefined);
    const view = renderHook(() => {
      const { channel } = useChannel(r.rt, "chat:lobby");
      return useChannelPresence(channel);
    });
    const [members, setPresence] = view.result.current;
    await r.until(() => expect(members()).toHaveLength(0));
    setPresence({ name: "ann" } as never);
    await r.until(() => expect(members()).toHaveLength(1));
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    setPresence({ name: "bob" } as never);
    await r.until(() => expect(members()[0]?.s).toEqual({ name: "bob" }));
    setPresence(undefined);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(0));
    setPresence("again" as never);
    await r.until(() => expect(watcher.presence.members()).toHaveLength(1));
    view.dispose();
    await r.until(() => expect(watcher.presence.members()).toHaveLength(0));
  });
});

describe("useRealtimeStatus", () => {
  it("[RT-C-47] follows the connection status and stops listening on dispose", async () => {
    const r = make();
    const view = renderHook(() => useRealtimeStatus(r.rt));
    expect(view.result.current()).toBe("connecting");
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await r.until(() => expect(view.result.current()).toBe("open"));
    view.dispose();
    r.sockets.live()[0]?.drop();
    await r.until(() => expect(r.rt.status).toBe("reconnecting"));
    expect(view.result.current()).toBe("open");
  });
});

describe("the browser coming back online", () => {
  it("[RT-R10] reconnects at once instead of waiting out the backoff, and stops listening after close", async () => {
    const r = make({ reconnect: { random: () => 1, minDelayMs: 30_000, maxDelayMs: 30_000 } });
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await r.until(() => expect(r.rt.status).toBe("open"));
    r.sockets.live()[0]?.drop();
    await r.until(() => expect(r.rt.status).toBe("reconnecting"));
    expect(r.sockets.sockets).toHaveLength(1);
    window.dispatchEvent(new Event("online"));
    await r.until(() => expect(r.rt.status).toBe("open"));
    expect(r.sockets.sockets).toHaveLength(2);
    r.rt.close();
    const standalone = createRealtime({ url: "http://localhost/rt", WebSocket: r.sockets.Socket });
    standalone.close();
    window.dispatchEvent(new Event("online"));
    expect(r.sockets.sockets).toHaveLength(2);
  });
});

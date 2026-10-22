import { describe, expect, it } from "vitest";
import { createRealtime } from "../src/client/index";
import { createHub } from "../src/server/index";
import { memoryWebSocket } from "../src/testing";

type Schema = { [name: `chat:${string}`]: { events: { message: { text: string } } } };

describe("memoryWebSocket", () => {
  it("[RT-C-60] runs a client against a real hub with no network: publish, ack, receive", async () => {
    const hub = createHub({
      channels: { "chat:{room}": { subscribe: true, publish: true } },
      onError: () => undefined,
    });
    const rt = createRealtime<Schema>({
      url: "/rt",
      transport: "ws",
      WebSocket: memoryWebSocket(hub),
    });
    const lobby = rt.channel("chat:lobby");
    const received: string[] = [];
    lobby.on("message", (data) => received.push(data.text));
    const ack = await lobby.publish("message", { text: "hi" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(ack.seq).toBe(1);
    expect(received).toEqual(["hi"]);
    rt.close();
    await hub.close();
  });
});

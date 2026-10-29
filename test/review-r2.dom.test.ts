import { afterEach, describe, expect, it } from "vitest";
import { type Rig, rig } from "./fixtures/harness";

const rigs: Rig[] = [];
afterEach(async () => {
  await Promise.all(rigs.splice(0).map((item) => item.dispose()));
});

function setVisibility(state: "visible" | "hidden"): void {
  Object.defineProperty(document, "visibilityState", { value: state, configurable: true });
  document.dispatchEvent(new Event("visibilitychange"));
}

describe("the tab becoming visible", () => {
  it("[RT-R2-7] reconnects at once when the tab shows again while waiting out a backoff, and not while hidden", async () => {
    const r = rig({ reconnect: { random: () => 1, minDelayMs: 30_000, maxDelayMs: 30_000 } });
    rigs.push(r);
    r.rt.channel("chat:lobby").on("message", () => undefined);
    await r.until(() => expect(r.rt.status).toBe("open"));
    r.sockets.live()[0]?.drop();
    await r.until(() => expect(r.rt.status).toBe("reconnecting"));
    setVisibility("hidden");
    await Promise.resolve();
    expect(r.sockets.sockets).toHaveLength(1);
    setVisibility("visible");
    await r.until(() => expect(r.rt.status).toBe("open"));
    expect(r.sockets.sockets).toHaveLength(2);
    r.rt.close();
    setVisibility("visible");
    expect(r.sockets.sockets).toHaveLength(2);
  });
});

import { describe, expect, it } from "vitest";
import { createRealtime } from "../src/client/index";

/**
 * Node 22's WebSocket fires only `error` for a refused upgrade (no `close`, readyState stays 0); browsers fire `error` then `close`.
 * The client must treat an error before open as the close, or it waits for ever.
 */
class ErrorOnlySocket {
  static opened = 0;
  onopen: (() => void) | null = null;
  onmessage: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 0;
  constructor() {
    ErrorOnlySocket.opened += 1;
    queueMicrotask(() => this.onerror?.());
  }
  send(): void {
    throw new Error("not open");
  }
  close(): void {
    /* a socket that never opened has nothing to close */
  }
}

describe("a refused upgrade that fires only error", () => {
  it("[RT-C-61] counts as a close: the client retries, and with transport auto falls back to SSE after two", async () => {
    ErrorOnlySocket.opened = 0;
    const requests: string[] = [];
    const rt = createRealtime<{ [name: `c:${string}`]: { events: { m: number } } }>({
      url: "http://localhost/rt",
      transport: "auto",
      WebSocket: ErrorOnlySocket as unknown as typeof WebSocket,
      fetch: (async (input: RequestInfo | URL) => {
        requests.push(String(input));
        return new Response(null, { status: 503 });
      }) as typeof fetch,
      reconnect: { minDelayMs: 5, maxDelayMs: 5, random: () => 0 },
      onError: () => undefined,
    });
    rt.channel("c:1").on("m", () => undefined);
    await new Promise((resolve) => setTimeout(resolve, 150));
    rt.close();
    expect(ErrorOnlySocket.opened).toBeGreaterThanOrEqual(2);
    expect(requests.length).toBeGreaterThan(0);
  });
});

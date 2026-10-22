import { afterEach, describe, expect, it, vi } from "vitest";
import { MAX_ENTRY_BYTES } from "../src/cloudflare/wire";
import { FakeObjectHost } from "./support/do-host";

const hosts: FakeObjectHost[] = [];
function host(): FakeObjectHost {
  const created = new FakeObjectHost();
  hosts.push(created);
  return created;
}
afterEach(() => {
  for (const created of hosts.splice(0)) created.dispose();
});

const rules = { history: 100, historyTtlMs: 60_000, idempotencyTtlMs: 60_000 };

async function post(
  target: FakeObjectHost,
  op: string,
  body: unknown,
  init: { method?: string; raw?: string } = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const method = init.method ?? "POST";
  const response = await target.object.fetch(
    new Request(`https://realtime.internal/v1/${op}`, {
      method,
      ...(method === "POST" ? { body: init.raw ?? JSON.stringify(body) } : {}),
    }),
  );
  return { status: response.status, body: (await response.json()) as Record<string, unknown> };
}

async function open(target: FakeObjectHost, query: string, upgrade = true): Promise<Response> {
  return target.object.fetch(
    new Request(`https://realtime.internal/v1/ws?${query}`, {
      headers: upgrade ? { upgrade: "websocket" } : {},
    }),
  );
}

describe("channel object: request protocol", () => {
  it("[RT-CF-60] publishes and reads history over the JSON protocol", async () => {
    const target = host();
    const published = await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: '"x"', publisher: "p" },
      options: rules,
    });
    expect(published).toEqual({ status: 200, body: { seq: 1, duplicate: false } });
    const history = await post(target, "history", { channel: "a", after: 0, limit: 10 });
    expect(history.status).toBe(200);
    expect(history.body["head"]).toBe(1);
    expect((history.body["messages"] as { data: string }[])[0]?.data).toBe('"x"');
  });

  it("[RT-CF-61] answers a refusal with a code and no internals for malformed requests", async () => {
    const target = host();
    expect((await post(target, "nothing", {})).status).toBe(404);
    expect((await post(target, "publish", {}, { method: "GET" })).status).toBe(405);
    expect((await post(target, "publish", null, { raw: "not json" })).status).toBe(400);
    const badChannel = await post(target, "history", { channel: "bad name", after: 0, limit: 1 });
    expect(badChannel).toEqual({
      status: 400,
      body: { error: { code: "bad_channel", message: "bad channel name" } },
    });
    expect((await post(target, "history", { channel: "a", after: -1, limit: 1 })).status).toBe(400);
    expect(
      (
        await post(target, "publish", {
          channel: "a",
          entry: { event: "bad event", data: "1" },
          options: rules,
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await post(target, "publish", {
          channel: "a",
          entry: { event: "e", data: "1" },
          options: { ...rules, history: 10_001 },
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await post(target, "presence-set", {
          channel: "a",
          member: { id: "m", v: 0, s: null },
          ttlMs: 1000,
        })
      ).status,
    ).toBe(400);
    expect(
      (await post(target, "presence-touch", { channel: "a", ids: [1], ttlMs: 1000 })).status,
    ).toBe(400);
  });

  it("[RT-CF-62] refuses a message that does not fit one stored value with too_large, and accepts one at the limit", async () => {
    const target = host();
    const fits = "x".repeat(MAX_ENTRY_BYTES - 1);
    expect(
      (
        await post(target, "publish", {
          channel: "a",
          entry: { event: "e", data: fits },
          options: rules,
        })
      ).status,
    ).toBe(200);
    const big = await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "x".repeat(MAX_ENTRY_BYTES + 1) },
      options: rules,
    });
    expect(big.status).toBe(413);
    expect((big.body["error"] as { code: string }).code).toBe("too_large");
    const huge = await post(target, "publish", null, { raw: "x".repeat(300_000) });
    expect(huge.status).toBe(413);
  });

  it("[RT-CF-63] a channel other than the object's is a conflict", async () => {
    const target = host();
    await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    const other = await post(target, "publish", {
      channel: "b",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    expect(other.status).toBe(409);
  });

  it("[RT-CF-64] a storage failure is `unavailable` with no internal message, and the object works again afterward", async () => {
    const target = host();
    target.storage.failNext(1, (operation) => operation === "transaction");
    const failed = await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    expect(failed.status).toBe(500);
    expect(JSON.stringify(failed.body)).not.toContain("injected");
    expect((failed.body["error"] as { code: string }).code).toBe("unavailable");
    const logged = vi.mocked(console.error);
    expect(logged.mock.calls).toHaveLength(1);
    logged.mockClear();
    const ok = await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    expect(ok.body).toEqual({ seq: 1, duplicate: false });
  });
});

describe("channel object: subscribe sockets and hibernation", () => {
  it("[RT-CF-70] needs an upgrade request, a valid channel and a valid after", async () => {
    const target = host();
    expect((await open(target, "c=a", false)).status).toBe(426);
    expect((await open(target, "c=bad%20name")).status).toBe(400);
    expect((await open(target, "c=a&after=-1")).status).toBe(400);
    expect((await open(target, "c=a&after=abc")).status).toBe(400);
    expect((await open(target, "")).status).toBe(400);
    expect((await open(target, "c=a")).status).toBe(101);
  });

  it("[RT-CF-71] keeps delivering to a socket after the object instance is evicted, and answers ping with pong", async () => {
    const target = host();
    const response = await open(target, "c=a");
    const client = Reflect.get(response, "webSocket") as {
      received: string[];
      accept(): void;
      send(text: string): void;
    };
    client.accept();
    await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    target.evict();
    expect(target.instances).toBe(2);
    await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "2" },
      options: rules,
    });
    client.send("ping");
    const frames = client.received.map((text) =>
      text === "pong" ? text : (JSON.parse(text) as { t: string }).t,
    );
    expect(frames).toEqual(["ready", "snap", "msg", "msg", "pong"]);
  });

  it("[RT-CF-72] a closed socket is forgotten, and a socket that failed is closed", async () => {
    const target = host();
    const first = Reflect.get(await open(target, "c=a"), "webSocket") as {
      accept(): void;
      close(code?: number): void;
    };
    const second = Reflect.get(await open(target, "c=a"), "webSocket") as { accept(): void };
    first.accept();
    second.accept();
    first.close(1000);
    await post(target, "publish", {
      channel: "a",
      entry: { event: "e", data: "1" },
      options: rules,
    });
    const [one, two] = target.clients;
    expect(one?.received.filter((text) => text.includes('"msg"'))).toHaveLength(0);
    expect(two?.received.filter((text) => text.includes('"msg"'))).toHaveLength(1);
    const failing = [...target.sockets][0];
    expect(failing).toBeDefined();
    if (failing !== undefined) target.object.webSocketError(failing);
    expect(failing?.open).toBe(false);
    expect(failing?.closedWith?.code).toBe(1011);
  });

  it("[RT-CF-73] the alarm of a restarted object announces an expired member's leave to the sockets that stayed connected", async () => {
    vi.useFakeTimers({ now: 2_000_000 });
    try {
      const target = host();
      const client = Reflect.get(await open(target, "c=a"), "webSocket") as {
        received: string[];
        accept(): void;
      };
      client.accept();
      await post(target, "presence-set", {
        channel: "a",
        member: { id: "m1", v: 1, s: "here" },
        ttlMs: 1000,
        graceMs: 500,
      });
      target.evict();
      expect(await target.tick()).toBe(false);
      vi.setSystemTime(2_001_600);
      expect(await target.tick()).toBe(true);
      await vi.advanceTimersByTimeAsync(0);
      const events = client.received.map((text) => JSON.parse(text) as { t: string; op?: string });
      expect(events.filter((event) => event.t === "pres").map((event) => event.op)).toEqual([
        "join",
        "leave",
      ]);
      expect(target.storage.alarmAt).toBe(null);
    } finally {
      vi.useRealTimers();
    }
  });
});

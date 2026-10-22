import { afterAll, afterEach, beforeAll } from "vitest";
import type { Adapter, PublishOptions } from "../../src/adapter";
import type { StoredEntry } from "../../src/protocol";
import { type RedisAdapterOptions, redisAdapter } from "../../src/redis";
import { RespClient } from "./resp";
import { clean, startServer, type TestServer, uniquePrefix } from "./server";

/** An adapter with the connections it uses, for tests that reach under it. */
export interface Opened {
  readonly adapter: Adapter;
  readonly client: RespClient;
  /** Everything the adapter reported through `onError`. */
  readonly errors: { readonly error: unknown; readonly context: { readonly where: string } }[];
}

/** What a test file gets: a server for the file, a fresh prefix per test, and adapters that are closed after each test. */
export interface Fixture {
  readonly server: () => TestServer;
  /** The prefix of the current test. */
  readonly prefix: () => string;
  /** Opens an adapter on the current test's prefix (or `options.prefix`). */
  open(options?: Partial<Omit<RedisAdapterOptions, "client">>): Opened;
  /** A connection for commands the test sends itself. */
  raw(): RespClient;
}

/** Registers the hooks that start the server, give every test its prefix and close what it opened. */
export function useRedis(): Fixture {
  let server: TestServer;
  let current = uniquePrefix();
  const used: string[] = [];
  const opened: Opened[] = [];
  const clients: RespClient[] = [];

  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(async () => {
    for (const prefix of used) await clean(server, prefix);
    await server.stop();
  });
  afterEach(async () => {
    await Promise.all(
      opened.splice(0).map(({ adapter }) => adapter.close().catch(() => undefined)),
    );
    await Promise.all(clients.splice(0).map((client) => client.close()));
    current = uniquePrefix();
  });

  return {
    server: () => server,
    prefix: () => current,
    open(options = {}) {
      const client = new RespClient(server.endpoint);
      clients.push(client);
      const errors: Opened["errors"][number][] = [];
      const prefix = options.prefix ?? current;
      used.push(prefix);
      const adapter = redisAdapter({
        client,
        prefix,
        onError: (error, context) => errors.push({ error, context }),
        ...options,
      });
      const result: Opened = { adapter, client, errors };
      opened.push(result);
      return result;
    },
    raw() {
      const client = new RespClient(server.endpoint);
      clients.push(client);
      return client;
    },
  };
}

export const RULES: PublishOptions = {
  history: 100,
  historyTtlMs: 60_000,
  idempotencyTtlMs: 60_000,
};

export const entry = (n: number | string): StoredEntry => ({ event: "e", data: JSON.stringify(n) });

/** Polls until `condition` holds. */
export async function until(condition: () => boolean, ms = 5000): Promise<void> {
  const stop = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > stop) throw new Error("the condition did not become true in time");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

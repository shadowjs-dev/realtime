import { vi } from "vitest";
import { createRealtime } from "../../src/client/index";
import type { Realtime, RealtimeOptions, Status } from "../../src/client/index";
import { createHub, type Hub, type HubOptions } from "../../src/server/index";
import type { Identity } from "../../src/server/rules";
import { connectTest, type TestConnection } from "../../src/testing";
import { fetchFactory, type FetchFactory } from "./memory-fetch";
import { type SocketFactory, socketFactory } from "./memory-socket";

/** The channel rules most client tests use. */
export const rules: NonNullable<HubOptions["channels"]> = {
  "chat:{room}": {
    subscribe: true,
    publish: true,
    presence: true,
    history: 100,
    handlers: {
      shout: ({ data }) => String(data).toUpperCase(),
      slow: () => new Promise((resolve) => setTimeout(() => resolve("late"), 500)),
      boom: () => {
        throw new Error("secret internals");
      },
    },
  },
  "tiny:{id}": { subscribe: true, publish: true, history: 2 },
  "readonly:{id}": { subscribe: true, publish: false },
  "secret:{id}": { subscribe: false },
};

export interface Rig {
  /** The hub clients are connected to now. */
  readonly hub: Hub;
  readonly errors: unknown[];
  readonly statuses: Status[];
  readonly sockets: SocketFactory;
  readonly fetches: FetchFactory;
  readonly rt: Realtime;
  /** A client of the same hub with no client library in between, for the "other user". */
  peer(): TestConnection;
  /** Starts another client library instance on the same hub and sockets. */
  another(options?: Partial<RealtimeOptions>): Realtime;
  /** Waits until `check` stops throwing. */
  until(check: () => void, timeoutMs?: number): Promise<void>;
  /** Closes the current hub (clients get 1001) and starts another on the same sockets; with `adapter` the log carries over. */
  restart(adapter?: HubOptions["adapter"]): Promise<Hub>;
  dispose(): Promise<void>;
}

/** Builds a hub, a client wired to it through in-memory sockets, and the lists a test reads. */
export function rig(
  client: Partial<RealtimeOptions> = {},
  hubOptions: Partial<HubOptions> = {},
  identity: Identity = {},
): Rig {
  const errors: unknown[] = [];
  const statuses: Status[] = [];
  const hubs: Hub[] = [createHub({ channels: rules, onError: () => undefined, ...hubOptions })];
  const current = (): Hub => hubs[hubs.length - 1] as Hub;
  const sockets = socketFactory(current, identity);
  const fetches = fetchFactory(current, identity);
  const opened: Realtime[] = [];
  const peers: TestConnection[] = [];
  const make = (options: Partial<RealtimeOptions>): Realtime => {
    const rt = createRealtime({
      url: "http://localhost/rt",
      transport: "ws",
      WebSocket: sockets.Socket,
      fetch: fetches.fetch,
      reconnect: { random: () => 0.5 },
      onError: (error) => errors.push(error),
      ...options,
    });
    opened.push(rt);
    return rt;
  };
  const rt = make({ onStatus: (status) => statuses.push(status), ...client });
  return {
    get hub() {
      return current();
    },
    errors,
    statuses,
    sockets,
    fetches,
    rt,
    peer() {
      const connection = connectTest(current());
      peers.push(connection);
      return connection;
    },
    another: make,
    until: (check, timeoutMs = 3000) => vi.waitFor(check, { timeout: timeoutMs, interval: 2 }),
    async restart(adapter) {
      await current().close();
      const next = createHub({
        channels: rules,
        onError: () => undefined,
        ...hubOptions,
        ...(adapter === undefined ? {} : { adapter }),
      });
      hubs.push(next);
      return next;
    },
    async dispose() {
      for (const item of opened) item.close();
      await Promise.all(peers.map((peer) => peer.close()));
      await Promise.all(hubs.map((item) => item.close()));
    },
  };
}

/**
 * What the Redis adapter needs from a Redis library, and the two wrappers that adapt the common ones. The adapter has no
 * dependency on a Redis package: it is written against these small interfaces, which ioredis and node-redis satisfy through
 * {@link fromIoredis} and {@link fromNodeRedis}, and which anything else (Upstash over TCP, a mock, your own pool) can implement.
 */

/**
 * A connection that runs commands. Commands issued without waiting for the previous one are pipelined by the library, which the
 * adapter relies on for throughput.
 *
 * @example
 * import type { RedisClient } from "@shadow-js/realtime/redis";
 *
 * declare function send(args: string[]): Promise<unknown>;
 * const client: RedisClient = { sendCommand: (args) => send([...args]) };
 * @since 1.0.0
 */
export interface RedisClient {
  /**
   * Runs one command (`["EVALSHA", sha, "1", key, ...]`) and resolves with its reply: a string (or bytes) for bulk and status
   * replies, a number for integers, `null` for nil, an array for multi-bulk. Rejects with the server's error (its message starts
   * with the error code, for example `NOSCRIPT`) or with the transport's error.
   */
  sendCommand(args: readonly string[]): Promise<unknown>;
  /**
   * Opens a dedicated connection for pub/sub, used when {@link RedisAdapterOptions.subscriber} is not given. The adapter owns it
   * and closes it with {@link RedisSubscriber.close}.
   */
  createSubscriber?(): Promise<RedisSubscriber> | RedisSubscriber;
}

/**
 * A connection in subscriber mode.
 *
 * @example
 * import type { RedisSubscriber } from "@shadow-js/realtime/redis";
 *
 * declare const subscriber: RedisSubscriber;
 * await subscriber.subscribe("rt:{room}:c", (message) => message.length);
 * await subscriber.unsubscribe("rt:{room}:c");
 * @since 1.0.0
 */
export interface RedisSubscriber {
  /**
   * Subscribes to a topic. Resolves once the server confirmed. Calling it again for a topic already subscribed replaces the
   * listener and does not deliver twice.
   */
  subscribe(topic: string, listener: (message: string) => void): Promise<void>;
  /** Stops listening to a topic. The listener is removed before the returned promise settles. */
  unsubscribe(topic: string): Promise<void>;
  /**
   * Registers a function to call whenever the connection was lost and came back (not for the first connection). The adapter
   * subscribes its topics again and recovers what was published in between from history. Returns a function that removes the
   * registration.
   */
  onReconnect(handler: () => void): () => void;
  /** Registers a function that receives the connection's errors, so none goes unhandled. Returns a function that removes it. */
  onError?(handler: (error: unknown) => void): () => void;
  /** Closes the connection. Called only for a subscriber the adapter created itself. */
  close?(): Promise<void>;
}

/**
 * The part of an ioredis connection that {@link fromIoredis} uses. The real `Redis` and `Cluster` classes fit.
 * @since 1.0.0
 */
export interface IoredisLike {
  call(command: string, ...args: string[]): Promise<unknown>;
  duplicate(): IoredisSubscriberLike;
}

/**
 * The part of a duplicated ioredis connection that {@link fromIoredis} uses in subscriber mode.
 * @since 1.0.0
 */
export interface IoredisSubscriberLike {
  subscribe(...topics: string[]): Promise<unknown>;
  unsubscribe(...topics: string[]): Promise<unknown>;
  on(event: string, listener: (...args: never[]) => void): unknown;
  off?(event: string, listener: (...args: never[]) => void): unknown;
  removeListener?(event: string, listener: (...args: never[]) => void): unknown;
  quit(): Promise<unknown>;
  disconnect?(): void;
}

/**
 * The part of a node-redis (v4 or v5) client that {@link fromNodeRedis} uses.
 * @since 1.0.0
 */
export interface NodeRedisLike {
  sendCommand(args: string[]): Promise<unknown>;
  duplicate(): NodeRedisSubscriberLike;
}

/**
 * The part of a duplicated node-redis client that {@link fromNodeRedis} uses in subscriber mode.
 * @since 1.0.0
 */
export interface NodeRedisSubscriberLike {
  connect(): Promise<unknown>;
  subscribe(topic: string, listener: (message: string, topic: string) => void): Promise<unknown>;
  unsubscribe(topic: string): Promise<unknown>;
  on(event: string, listener: (...args: never[]) => void): unknown;
  off?(event: string, listener: (...args: never[]) => void): unknown;
  removeListener?(event: string, listener: (...args: never[]) => void): unknown;
  close?(): Promise<unknown>;
  quit?(): Promise<unknown>;
  disconnect?(): Promise<unknown>;
}

/** A set of handlers with add-and-remove, shared by the two wrappers. */
function handlers<A extends unknown[]>(): {
  add(handler: (...args: A) => void): () => void;
  fire(...args: A): void;
} {
  const set = new Set<(...args: A) => void>();
  return {
    add(handler) {
      set.add(handler);
      return () => {
        set.delete(handler);
      };
    },
    fire(...args) {
      for (const handler of [...set]) {
        try {
          handler(...args);
        } catch {
          // A handler's failure is its own.
        }
      }
    },
  };
}

/**
 * Adapts an ioredis connection (`Redis` or `Cluster`). Commands go through `call`; the subscriber is a `duplicate()` of the
 * connection, which ioredis re-subscribes by itself after a reconnect (the adapter then recovers what was missed).
 *
 * @param client The ioredis connection.
 * @returns A {@link RedisClient} for `redisAdapter`.
 * @example
 * import { Redis } from "ioredis";
 * import { fromIoredis, redisAdapter } from "@shadow-js/realtime/redis";
 *
 * const adapter = redisAdapter({ client: fromIoredis(new Redis(process.env.REDIS_URL ?? "")) });
 * await adapter.close();
 * @since 1.0.0
 */
export function fromIoredis(client: IoredisLike): RedisClient {
  return {
    sendCommand: (args) => {
      const [command, ...rest] = args;
      return client.call(command ?? "", ...rest);
    },
    createSubscriber: (): RedisSubscriber => {
      const connection = client.duplicate();
      const topics = new Map<string, (message: string) => void>();
      const reconnects = handlers<[]>();
      const errors = handlers<[unknown]>();
      let ready = 0;
      connection.on("message", ((topic: string, message: string) => {
        topics.get(topic)?.(message);
      }) as (...args: never[]) => void);
      connection.on("ready", (() => {
        ready += 1;
        if (ready > 1) reconnects.fire();
      }) as (...args: never[]) => void);
      connection.on("error", ((error: unknown) => errors.fire(error)) as (
        ...args: never[]
      ) => void);
      return {
        async subscribe(topic, listener) {
          topics.set(topic, listener);
          await connection.subscribe(topic);
        },
        async unsubscribe(topic) {
          topics.delete(topic);
          await connection.unsubscribe(topic);
        },
        onReconnect: (handler) => reconnects.add(handler),
        onError: (handler) => errors.add(handler),
        async close() {
          topics.clear();
          try {
            await connection.quit();
          } catch {
            connection.disconnect?.();
          }
        },
      };
    },
  };
}

/**
 * Adapts a node-redis (v4 or v5) client. Commands go through `sendCommand`; the subscriber is a `duplicate()` that is connected
 * here and that node-redis re-subscribes by itself after a reconnect (the adapter then recovers what was missed). Pass a
 * standalone client; node-redis cluster clients route by key and need their own wrapper.
 *
 * @param client The node-redis client (already connected).
 * @returns A {@link RedisClient} for `redisAdapter`.
 * @example
 * import { createClient } from "redis";
 * import { fromNodeRedis, redisAdapter } from "@shadow-js/realtime/redis";
 *
 * const client = createClient({ url: process.env.REDIS_URL ?? "" });
 * await client.connect();
 * const adapter = redisAdapter({ client: fromNodeRedis(client) });
 * await adapter.close();
 * @since 1.0.0
 */
export function fromNodeRedis(client: NodeRedisLike): RedisClient {
  return {
    sendCommand: (args) => client.sendCommand([...args]),
    createSubscriber: async (): Promise<RedisSubscriber> => {
      const connection = client.duplicate();
      const topics = new Map<string, (message: string) => void>();
      const dispatch = (message: string, topic: string): void => {
        topics.get(topic)?.(message);
      };
      const reconnects = handlers<[]>();
      const errors = handlers<[unknown]>();
      let ready = 0;
      connection.on("ready", (() => {
        ready += 1;
        if (ready > 1) reconnects.fire();
      }) as (...args: never[]) => void);
      // node-redis throws an unhandled "error" event as an exception: it is always handled here.
      connection.on("error", ((error: unknown) => errors.fire(error)) as (
        ...args: never[]
      ) => void);
      await connection.connect();
      const registered = new Set<string>();
      return {
        async subscribe(topic, listener) {
          topics.set(topic, listener);
          if (registered.has(topic)) return;
          registered.add(topic);
          try {
            await connection.subscribe(topic, dispatch);
          } catch (error) {
            registered.delete(topic);
            topics.delete(topic);
            throw error;
          }
        },
        async unsubscribe(topic) {
          topics.delete(topic);
          if (!registered.delete(topic)) return;
          await connection.unsubscribe(topic);
        },
        onReconnect: (handler) => reconnects.add(handler),
        onError: (handler) => errors.add(handler),
        async close() {
          topics.clear();
          registered.clear();
          if (connection.close !== undefined) await connection.close();
          else if (connection.quit !== undefined) await connection.quit();
          else await connection.disconnect?.();
        },
      };
    },
  };
}

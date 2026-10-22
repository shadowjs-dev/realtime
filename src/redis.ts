import type {
  Adapter,
  HistoryResult,
  PresenceRef,
  PublishOptions,
  PublishResult,
  Unsubscribe,
} from "./adapter";
import { RealtimeError } from "./errors";
import type { Member, PresenceEvent, StoredEntry, StoredMessage } from "./protocol";
import type { RedisClient, RedisSubscriber } from "./redis/client";
import {
  HISTORY,
  KEEPALIVE,
  PRESENCE_LIST,
  PRESENCE_REAP,
  PRESENCE_REMOVE,
  PRESENCE_SET,
  PRESENCE_TOUCH,
  PUBLISH,
  type Script,
} from "./redis/scripts";
import { createTopics, type RedisErrorContext } from "./redis/topics";

export {
  fromIoredis,
  fromNodeRedis,
  type IoredisLike,
  type IoredisSubscriberLike,
  type NodeRedisLike,
  type NodeRedisSubscriberLike,
  type RedisClient,
  type RedisSubscriber,
} from "./redis/client";
export type { RedisErrorContext } from "./redis/topics";

/**
 * Options of {@link redisAdapter}.
 * @since 1.0.0
 */
export interface RedisAdapterOptions {
  /** The connection that runs commands (see {@link fromIoredis} and {@link fromNodeRedis}). The adapter never closes it. */
  readonly client: RedisClient;
  /**
   * A connection in subscriber mode, shared by every subscription of this adapter. Without it the adapter asks the client for one
   * (`client.createSubscriber`) when the first subscription is made, and closes that one with the adapter. A subscriber you pass
   * is yours to close; give it to one adapter only (a topic has one listener on a subscriber).
   */
  readonly subscriber?: RedisSubscriber;
  /**
   * Prefix of every key and topic. Servers that share channels use the same one; apps (or environments) that must not share
   * use different ones. No braces or whitespace. Default `"rt"`.
   */
  readonly prefix?: string;
  /**
   * Milliseconds a channel's numbering outlives its last publish (and its last listener, which keeps it alive). After an idle
   * channel expired, its seq restarts at 1, which a client that resumes sees as `reset`. Default 24 hours.
   */
  readonly channelTtlMs?: number;
  /**
   * Milliseconds the presence keys of a channel outlive the last refresh of any member, so a channel nobody reaps anymore does
   * not stay in Redis forever. Default 60 seconds.
   */
  readonly presenceGraceMs?: number;
  /**
   * Receives every failure the adapter handled without surfacing it: a malformed stored entry that was skipped, a listener that
   * threw, a subscriber connection error, a recovery that gave up. Default: ignored.
   */
  readonly onError?: (error: unknown, context: RedisErrorContext) => void;
}

const DAY_MS = 86_400_000;
const MAX_LIMIT = 10_000;
const MAX_KEY_BYTES = 1024;
const REAP_PAGE = 500;
const TOUCH_PAGE = 500;
const PIPELINE = 64;

function positive(name: string, value: number): number {
  if (!Number.isFinite(value) || value < 1)
    throw new RangeError(`${name} must be a positive number of milliseconds.`);
  return Math.ceil(value);
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(reply: unknown): string {
  if (typeof reply === "string") return reply;
  if (reply instanceof Uint8Array) return new TextDecoder().decode(reply);
  if (typeof reply === "number") return String(reply);
  throw new TypeError("Redis answered with an unexpected reply.");
}

function integer(reply: unknown): number {
  const value = typeof reply === "number" ? reply : Number(text(reply));
  if (!Number.isSafeInteger(value))
    throw new TypeError("Redis answered with an unexpected number.");
  return value;
}

function list(reply: unknown): readonly unknown[] {
  if (!Array.isArray(reply)) throw new TypeError("Redis answered with an unexpected reply.");
  return reply;
}

function isNoScript(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes("NOSCRIPT");
}

/** Walks pages of `size` items and runs `work` on up to `PIPELINE` of them at once. */
async function inBatches<T>(items: readonly T[], work: (item: T) => Promise<void>): Promise<void> {
  for (let from = 0; from < items.length; from += PIPELINE)
    await Promise.all(items.slice(from, from + PIPELINE).map(work));
}

/**
 * The adapter that makes several servers one: messages, history, idempotency and presence live in Redis (6.2 or later, or
 * Valkey), and fan-out between servers goes through pub/sub. A hub created with it behaves exactly like one over the in-memory
 * adapter, on as many servers as you run.
 *
 * What is stored, per channel `c` (`p` is the prefix; the braces make every key of one channel hash to one Redis Cluster slot):
 * `p:{c}:m` (hash: head seq, history lifetime), `p:{c}:l` and `p:{c}:t` (sorted sets: the retained messages by seq, and when
 * each was accepted), `p:{c}:k:<key>` (idempotency keys), `p:{c}:p` and `p:{c}:x` (presence members, and when each expires),
 * and the topics `p:{c}:c` (messages) and `p:{c}:e` (presence events). A publish is one Lua script: it checks the key,
 * numbers, stores, trims by count and age, and publishes, atomically, so seq order is delivery order. Every method rejects with
 * `RealtimeError("unavailable")` when Redis cannot be reached, and leaves no half-applied state.
 *
 * @param options The connections and the key prefix.
 * @returns The adapter. Close it with the hub (`createHub` closes an adapter it created; close one you passed yourself).
 * @throws RangeError for an option that is not valid; TypeError when no subscriber is given and the client cannot create one.
 * @example
 * import { Redis } from "ioredis";
 * import { createHub } from "@shadow-js/realtime/server";
 * import { fromIoredis, redisAdapter } from "@shadow-js/realtime/redis";
 *
 * const adapter = redisAdapter({ client: fromIoredis(new Redis(process.env.REDIS_URL ?? "")), prefix: "myapp" });
 * const hub = createHub({ adapter, channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * await hub.close();
 * await adapter.close();
 * @since 1.0.0
 */
export function redisAdapter(options: RedisAdapterOptions): Adapter {
  const { client } = options;
  if (typeof client?.sendCommand !== "function")
    throw new TypeError(
      "redisAdapter needs a client with sendCommand (see fromIoredis, fromNodeRedis).",
    );
  if (options.subscriber === undefined && client.createSubscriber === undefined)
    throw new TypeError(
      "redisAdapter needs a subscriber: pass one, or a client that can create one (fromIoredis, fromNodeRedis).",
    );
  const prefix = options.prefix ?? "rt";
  if (!/^[^\s{}]{1,128}$/.test(prefix))
    throw new RangeError("prefix must be 1 to 128 characters without braces or whitespace.");
  const channelTtlMs = positive("channelTtlMs", options.channelTtlMs ?? DAY_MS);
  const graceMs = positive("presenceGraceMs", options.presenceGraceMs ?? 60_000);
  const onError = options.onError;
  let closed = false;
  let ownedSubscriber: Promise<RedisSubscriber> | undefined;

  function report(error: unknown, context: RedisErrorContext): void {
    try {
      onError?.(error, context);
    } catch {
      // The reporter's failure must not become the adapter's.
    }
  }

  function fail(error: unknown): RealtimeError {
    if (error instanceof RealtimeError) return error;
    return new RealtimeError(
      "unavailable",
      closed ? "the Redis adapter is closed." : "the Redis adapter could not complete the command.",
      { cause: error },
    );
  }

  function open(): void {
    if (closed) throw new RealtimeError("unavailable", "the Redis adapter is closed.");
  }

  function name(channel: string): string {
    if (channel.length === 0 || channel.includes("{") || channel.includes("}"))
      throw new RealtimeError("bad_channel", "a channel name is empty or contains a brace.");
    return `${prefix}:{${channel}}`;
  }

  const keysOf = (channel: string) => {
    const base = name(channel);
    return {
      meta: `${base}:m`,
      log: `${base}:l`,
      time: `${base}:t`,
      idem: (key: string): string => `${base}:k:${key}`,
      members: `${base}:p`,
      expiry: `${base}:x`,
      messages: `${base}:c`,
      events: `${base}:e`,
    };
  };

  /** Runs a script by hash and, when the server does not know it (first use, a flush, a failover), by source. */
  async function run(
    script: Script,
    keys: readonly string[],
    args: readonly string[],
  ): Promise<unknown> {
    open();
    const tail = [String(keys.length), ...keys, ...args];
    try {
      try {
        return await client.sendCommand(["EVALSHA", script.sha, ...tail]);
      } catch (error) {
        if (!isNoScript(error)) throw error;
        return await client.sendCommand(["EVAL", script.source, ...tail]);
      }
    } catch (error) {
      throw fail(error);
    }
  }

  // --- parsing what Redis holds --------------------------------------------------------------------------------------

  function parseMessage(source: string, channel: string): StoredMessage | undefined {
    try {
      const value: unknown = JSON.parse(source);
      if (isRecord(value)) {
        const seq = value["s"];
        const at = value["a"];
        const event = value["e"];
        const data = value["d"];
        const publisher = value["p"];
        const user = value["u"];
        if (
          typeof seq === "number" &&
          Number.isSafeInteger(seq) &&
          seq >= 1 &&
          typeof at === "number" &&
          typeof event === "string" &&
          typeof data === "string" &&
          (publisher === undefined || typeof publisher === "string") &&
          (user === undefined || typeof user === "string")
        )
          return {
            seq,
            at,
            event,
            data,
            ...(publisher === undefined ? {} : { publisher }),
            ...(user === undefined ? {} : { user }),
          };
      }
    } catch (error) {
      report(error, { where: "parse", channel });
      return undefined;
    }
    report(new TypeError("a stored message has an unexpected shape."), { where: "parse", channel });
    return undefined;
  }

  function memberOf(value: unknown): Member | undefined {
    if (!isRecord(value)) return undefined;
    const id = value["id"];
    const user = value["u"];
    const version = value["v"];
    if (
      typeof id !== "string" ||
      typeof version !== "number" ||
      !Number.isSafeInteger(version) ||
      !("s" in value) ||
      (user !== undefined && typeof user !== "string")
    )
      return undefined;
    return {
      id,
      ...(user === undefined ? {} : { u: user }),
      s: value["s"] as Member["s"],
      v: version,
    };
  }

  function parseStoredMember(source: unknown, channel: string): Member | undefined {
    try {
      const value: unknown = JSON.parse(text(source));
      const member = isRecord(value) ? memberOf(value["m"]) : undefined;
      if (member !== undefined) return member;
    } catch (error) {
      report(error, { where: "parse", channel });
      return undefined;
    }
    report(new TypeError("a stored presence member has an unexpected shape."), {
      where: "parse",
      channel,
    });
    return undefined;
  }

  function parsePresence(source: string, channel: string): PresenceEvent | undefined {
    try {
      const value: unknown = JSON.parse(source);
      if (isRecord(value)) {
        const op = value["o"];
        const member = memberOf(value["m"]);
        if ((op === "join" || op === "set" || op === "leave") && member !== undefined)
          return { channel, op, member };
      }
    } catch (error) {
      report(error, { where: "parse", channel });
      return undefined;
    }
    report(new TypeError("a presence event has an unexpected shape."), { where: "parse", channel });
    return undefined;
  }

  // --- reads shared with the subscription recovery -------------------------------------------------------------------

  async function history(channel: string, after: number, limit: number): Promise<HistoryResult> {
    const keys = keysOf(channel);
    const from = Number.isFinite(after)
      ? Math.min(Math.max(0, Math.floor(after)), Number.MAX_SAFE_INTEGER)
      : 0;
    const count = Number.isFinite(limit) ? Math.min(Math.max(0, Math.floor(limit)), MAX_LIMIT) : 0;
    const reply = list(
      await run(HISTORY, [keys.meta, keys.log, keys.time], [String(from), String(count)]),
    );
    const messages: StoredMessage[] = [];
    try {
      for (const row of reply.slice(2)) {
        const message = parseMessage(text(row), channel);
        if (message !== undefined) messages.push(message);
      }
      return { messages, head: integer(reply[0]), oldest: integer(reply[1]) };
    } catch (error) {
      throw fail(error);
    }
  }

  async function members(channel: string): Promise<readonly Member[]> {
    const keys = keysOf(channel);
    const reply = list(await run(PRESENCE_LIST, [keys.members, keys.expiry], []));
    const out: Member[] = [];
    for (const row of reply) {
      const member = parseStoredMember(row, channel);
      if (member !== undefined) out.push(member);
    }
    return out;
  }

  // --- the shared subscriber -----------------------------------------------------------------------------------------

  function subscriber(): Promise<RedisSubscriber> {
    open();
    if (options.subscriber !== undefined) return Promise.resolve(options.subscriber);
    if (ownedSubscriber === undefined) {
      const create = client.createSubscriber;
      if (create === undefined) return Promise.reject(fail(new TypeError("no subscriber.")));
      ownedSubscriber = (async () => create.call(client))();
      ownedSubscriber.catch(() => {
        ownedSubscriber = undefined;
      });
    }
    return ownedSubscriber.catch((error: unknown) => {
      throw fail(error);
    });
  }

  const topics = createTopics({
    subscriber,
    history: async (channel, after, limit) => (await history(channel, after, limit)).messages,
    members,
    keepAlive: async (channel) => {
      await run(KEEPALIVE, [keysOf(channel).meta], [String(channelTtlMs)]);
    },
    parseMessage,
    parsePresence,
    messageTopic: (channel) => keysOf(channel).messages,
    presenceTopic: (channel) => keysOf(channel).events,
    report,
    fail,
    keepAliveMs: Math.max(100, Math.floor(channelTtlMs / 3)),
  });

  // --- the adapter ---------------------------------------------------------------------------------------------------

  return {
    async publish(
      channel: string,
      entry: StoredEntry,
      publish: PublishOptions,
    ): Promise<PublishResult> {
      const keys = keysOf(channel);
      if (typeof entry.data !== "string")
        throw new TypeError("a stored entry's data is the JSON text of the value.");
      const history = Math.max(0, Math.floor(publish.history));
      const historyTtl = positive("historyTtlMs", publish.historyTtlMs);
      const idempotencyTtl = positive("idempotencyTtlMs", publish.idempotencyTtlMs);
      const key = publish.key;
      if (key !== undefined && (key.length === 0 || key.length > MAX_KEY_BYTES))
        throw new RealtimeError("bad_frame", "an idempotency key is empty or too long.");
      const body = JSON.stringify({
        e: entry.event,
        d: entry.data,
        ...(entry.publisher === undefined ? {} : { p: entry.publisher }),
        ...(entry.user === undefined ? {} : { u: entry.user }),
      }).slice(1);
      const reply = list(
        await run(
          PUBLISH,
          [keys.meta, keys.log, keys.time, ...(key === undefined ? [] : [keys.idem(key)])],
          [
            String(history),
            String(historyTtl),
            String(idempotencyTtl),
            String(channelTtlMs),
            keys.messages,
            body,
          ],
        ),
      );
      try {
        return { seq: integer(reply[0]), duplicate: integer(reply[1]) === 1 };
      } catch (error) {
        throw fail(error);
      }
    },

    subscribe(channel: string, listener: (message: StoredMessage) => void): Promise<Unsubscribe> {
      try {
        open();
        name(channel);
      } catch (error) {
        return Promise.reject(fail(error));
      }
      return topics.messages(channel, listener);
    },

    history,

    async presenceSet(channel: string, member: Member, ttlMs: number): Promise<void> {
      const keys = keysOf(channel);
      if (!Number.isSafeInteger(member.v) || member.v < 0)
        throw new RangeError("a member version must be a non-negative integer.");
      const json = JSON.stringify({
        id: member.id,
        ...(member.u === undefined ? {} : { u: member.u }),
        s: member.s === undefined ? null : member.s,
        v: member.v,
      });
      await run(
        PRESENCE_SET,
        [keys.members, keys.expiry],
        [
          member.id,
          String(member.v),
          String(positive("ttlMs", ttlMs)),
          keys.events,
          `{"v":${member.v},"m":${json}}`,
          json,
          String(graceMs),
        ],
      );
    },

    async presenceRemove(channel: string, id: string): Promise<boolean> {
      const keys = keysOf(channel);
      return (
        integer(await run(PRESENCE_REMOVE, [keys.members, keys.expiry], [id, keys.events])) === 1
      );
    },

    async presenceList(channel: string): Promise<readonly Member[]> {
      return members(channel);
    },

    async presenceTouch(
      entries: readonly PresenceRef[],
      ttlMs: number,
    ): Promise<readonly PresenceRef[]> {
      const ttl = String(positive("ttlMs", ttlMs));
      const byChannel = new Map<string, string[]>();
      for (const ref of entries) {
        const ids = byChannel.get(ref.channel);
        if (ids === undefined) byChannel.set(ref.channel, [ref.id]);
        else ids.push(ref.id);
      }
      const jobs: { readonly channel: string; readonly ids: readonly string[] }[] = [];
      for (const [channel, ids] of byChannel)
        for (let from = 0; from < ids.length; from += TOUCH_PAGE)
          jobs.push({ channel, ids: ids.slice(from, from + TOUCH_PAGE) });
      const missing: PresenceRef[] = [];
      await inBatches(jobs, async ({ channel, ids }) => {
        const keys = keysOf(channel);
        const reply = list(
          await run(PRESENCE_TOUCH, [keys.members, keys.expiry], [ttl, String(graceMs), ...ids]),
        );
        for (const id of reply) missing.push({ channel, id: text(id) });
      });
      return missing;
    },

    async presenceReap(channel: string): Promise<readonly Member[]> {
      const keys = keysOf(channel);
      const removed: Member[] = [];
      for (;;) {
        const reply = list(
          await run(PRESENCE_REAP, [keys.members, keys.expiry], [keys.events, String(REAP_PAGE)]),
        );
        for (const row of reply) {
          const member = parseStoredMember(row, channel);
          if (member !== undefined) removed.push(member);
        }
        if (reply.length < REAP_PAGE) return removed;
      }
    },

    presenceSubscribe(
      channel: string,
      listener: (event: PresenceEvent) => void,
    ): Promise<Unsubscribe> {
      try {
        open();
        name(channel);
      } catch (error) {
        return Promise.reject(fail(error));
      }
      return topics.presence(channel, listener);
    },

    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      await topics.close();
      const owned = ownedSubscriber;
      ownedSubscriber = undefined;
      if (owned !== undefined) {
        try {
          await (await owned).close?.();
        } catch (error) {
          report(error, { where: "close" });
        }
      }
    },
  };
}

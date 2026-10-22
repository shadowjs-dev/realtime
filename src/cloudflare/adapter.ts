import type {
  Adapter,
  HistoryResult,
  PresenceRef,
  PublishOptions,
  PublishResult,
  Unsubscribe,
} from "../adapter";
import { type ErrorCode, RealtimeError } from "../errors";
import type { Member, PresenceEvent, StoredEntry, StoredMessage } from "../protocol";
import { ChannelLink, type LinkHost } from "./link";
import type { ChannelNamespace, ChannelStub } from "./types";
import { isWholeNumber, parseMember, parseStored } from "./wire";

/**
 * Options of {@link cloudflareAdapter}.
 * @since 1.0.0
 */
export interface CloudflareRealtimeOptions {
  /** The Durable Object namespace binding of {@link RealtimeChannelObject} (`env.CHANNELS`). */
  readonly namespace: ChannelNamespace;
  /**
   * Separates this adapter's channels from other adapters on the same namespace (the object's name is the prefix, a newline and
   * the channel). Default: none, the object is named by the channel alone.
   */
  readonly prefix?: string;
  /** A Cloudflare location hint for objects this adapter creates (for example `"weur"`). */
  readonly locationHint?: string;
  /** Milliseconds one request to an object may take. Default: 10,000. */
  readonly requestTimeoutMs?: number;
  /** Milliseconds `subscribe` waits for the subscribe socket before it rejects with `unavailable`. Default: 10,000. */
  readonly connectTimeoutMs?: number;
  /**
   * How many times a request that is safe to repeat (history, presence reads and updates, a publish with an idempotency key) is
   * repeated after a transport failure. Default: 2.
   */
  readonly retries?: number;
  /** Backoff of the subscribe socket: full jitter from `minDelayMs` up to `maxDelayMs`. Defaults: 250 and 10,000. */
  readonly reconnect?: { readonly minDelayMs?: number; readonly maxDelayMs?: number };
  /** Milliseconds between keep-alive pings on a subscribe socket; 0 turns them off. Default: 30,000. */
  readonly keepAliveMs?: number;
  /**
   * Milliseconds after a presence member's expiry that the object's alarm waits before it announces the leave itself. The hub's
   * own `presenceReap` normally runs inside that window. Default: 2,000.
   */
  readonly presenceGraceMs?: number;
  /**
   * Forget a channel (numbering, history, keys) after this many milliseconds with no message, no presence and no subscriber.
   * A client that resumes later sees `reset`. Default: 0, never.
   */
  readonly idleMs?: number;
  /** The most events queued for one listener before newer ones are dropped (the hub then recovers from history). Default: 1,000. */
  readonly maxQueued?: number;
  /** Receives failures nobody else sees: a listener that threw. Default: `console.error`. */
  readonly onError?: (
    error: unknown,
    context: { readonly where: string; readonly channel: string },
  ) => void;
}

const ORIGIN = "https://realtime.internal";
const PAGE = 512;
/** Codes the object answers that are the caller's to see; everything else is `unavailable`. */
const PASS: ReadonlySet<string> = new Set<ErrorCode>([
  "too_large",
  "bad_channel",
  "bad_frame",
  "limit",
  "conflict",
]);

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unavailable(message: string, cause?: unknown): RealtimeError {
  return new RealtimeError("unavailable", message, cause === undefined ? undefined : { cause });
}

function count(value: unknown, name: string, positive = false): number {
  if (!isWholeNumber(value) || (positive && value < 1))
    throw new TypeError(`${name} must be a whole number`);
  return value;
}

function option(value: number | undefined, fallback: number, name: string): number {
  return value === undefined ? fallback : count(value, name);
}

/** Reads the object's reply. `parse` returns `undefined` for a shape it does not accept. */
type Reader<T> = (body: unknown) => T | undefined;

const readPublish: Reader<PublishResult> = (body) =>
  isRecord(body) &&
  isWholeNumber(body["seq"]) &&
  body["seq"] >= 1 &&
  typeof body["duplicate"] === "boolean"
    ? { seq: body["seq"], duplicate: body["duplicate"] }
    : undefined;

interface Page extends HistoryResult {
  readonly more: boolean;
}

const readHistory: Reader<Page> = (body) => {
  if (!isRecord(body) || !Array.isArray(body["messages"])) return undefined;
  const { head, oldest, more } = body;
  if (!isWholeNumber(head) || !isWholeNumber(oldest) || typeof more !== "boolean") return undefined;
  const messages: StoredMessage[] = [];
  for (const item of body["messages"]) {
    const message = parseStored(item);
    if (message === undefined) return undefined;
    messages.push(message);
  }
  return { messages, head, oldest, more };
};

const readMembers: Reader<Member[]> = (body) => {
  if (!isRecord(body) || !Array.isArray(body["members"])) return undefined;
  const members: Member[] = [];
  for (const item of body["members"]) {
    const member = parseMember(item);
    if (member === undefined) return undefined;
    members.push(member);
  }
  return members;
};

const readRemoved: Reader<boolean> = (body) =>
  isRecord(body) && typeof body["removed"] === "boolean" ? body["removed"] : undefined;

const readMissing: Reader<string[]> = (body) => {
  if (!isRecord(body) || !Array.isArray(body["missing"])) return undefined;
  return body["missing"].every((id) => typeof id === "string")
    ? (body["missing"] as string[])
    : undefined;
};

const readEmpty: Reader<true> = (body) => (isRecord(body) ? true : undefined);

/**
 * The adapter for Cloudflare Workers: each channel is one Durable Object ({@link RealtimeChannelObject}) that numbers,
 * retains and fans out its messages, so a channel is ordered and gapless wherever the Workers that use it run.
 *
 * `publish`, `history` and the presence calls are one request to the channel's object each. `subscribe` and
 * `presenceSubscribe` share one WebSocket per channel from this adapter to the object, which the adapter reopens with backoff
 * and resumes with the last seq it saw. Call `close()` when the adapter is no longer needed; it closes every socket and
 * cancels every timer.
 *
 * Within one Worker request, create the adapter where the hub lives. A Worker may touch a socket only in the request that
 * created it, so one hub that serves many client connections belongs inside a Durable Object, which keeps one context for all
 * of them; see the package's Cloudflare guide.
 *
 * @param options The namespace binding of the channel object and the tuning.
 * @returns The adapter.
 * @throws {TypeError} When an option is not valid.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 * import { cloudflareAdapter } from "@shadow-js/realtime/cloudflare";
 *
 * declare const env: { CHANNELS: Parameters<typeof cloudflareAdapter>[0]["namespace"] };
 * const adapter = cloudflareAdapter({ namespace: env.CHANNELS });
 * const hub = createHub({ adapter, channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * void hub.close();
 * @since 1.0.0
 */
export function cloudflareAdapter(options: CloudflareRealtimeOptions): Adapter {
  const { namespace } = options;
  if (typeof namespace?.idFromName !== "function" || typeof namespace.get !== "function")
    throw new TypeError("cloudflareAdapter: namespace must be a Durable Object namespace binding");
  const prefix = options.prefix;
  if (prefix !== undefined && (prefix.length === 0 || prefix.length > 256 || prefix.includes("\n")))
    throw new TypeError("cloudflareAdapter: prefix must be 1..256 characters without a newline");
  const requestTimeoutMs = option(options.requestTimeoutMs, 10_000, "requestTimeoutMs");
  const connectTimeoutMs = option(options.connectTimeoutMs, 10_000, "connectTimeoutMs");
  const retries = option(options.retries, 2, "retries");
  const minDelayMs = option(options.reconnect?.minDelayMs, 250, "reconnect.minDelayMs");
  const maxDelayMs = option(options.reconnect?.maxDelayMs, 10_000, "reconnect.maxDelayMs");
  const keepAliveMs = option(options.keepAliveMs, 30_000, "keepAliveMs");
  const graceMs = option(options.presenceGraceMs, 2000, "presenceGraceMs");
  const idleMs = option(options.idleMs, 0, "idleMs");
  const maxQueued = count(options.maxQueued ?? 1000, "maxQueued", true);
  if (requestTimeoutMs === 0 || connectTimeoutMs === 0)
    throw new TypeError("cloudflareAdapter: timeouts must be above 0");
  const report =
    options.onError ??
    ((error: unknown, context: { readonly where: string; readonly channel: string }): void =>
      console.error(`[realtime] ${context.where} ${context.channel}`, error));

  let closed = false;
  const links = new Map<string, ChannelLink>();
  const cancels = new Set<() => void>();
  const inflight = new Set<AbortController>();
  const sleepers = new Set<(error: unknown) => void>();

  function stubFor(channel: string): ChannelStub {
    const name = prefix === undefined ? channel : `${prefix}\n${channel}`;
    const id = namespace.idFromName(name);
    return options.locationHint === undefined
      ? namespace.get(id)
      : namespace.get(id, { locationHint: options.locationHint });
  }

  function after(ms: number, task: () => void): () => void {
    const handle = setTimeout(() => {
      cancels.delete(cancel);
      task();
    }, ms);
    (handle as { unref?: () => void }).unref?.();
    const cancel = (): void => {
      clearTimeout(handle);
      cancels.delete(cancel);
    };
    cancels.add(cancel);
    return cancel;
  }

  function every(ms: number, task: () => void): () => void {
    const handle = setInterval(task, ms);
    (handle as { unref?: () => void }).unref?.();
    const cancel = (): void => {
      clearInterval(handle);
      cancels.delete(cancel);
    };
    cancels.add(cancel);
    return cancel;
  }

  function delay(ms: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const stop = (error: unknown): void => {
        cancel();
        sleepers.delete(stop);
        reject(error);
      };
      const cancel = after(ms, () => {
        sleepers.delete(stop);
        resolve();
      });
      sleepers.add(stop);
    });
  }

  function assertOpen(): void {
    if (closed) throw new RealtimeError("closed", "the adapter is closed");
  }

  /**
   * One request to the channel's object. A transport failure, a 5xx or an unreadable reply is `unavailable`; with `safe` it is
   * repeated first, with a short growing pause.
   */
  async function call<T>(
    channel: string,
    op: string,
    body: Readonly<Record<string, unknown>>,
    read: Reader<T>,
    safe: boolean,
  ): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      assertOpen();
      const controller = new AbortController();
      inflight.add(controller);
      const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
      try {
        const response = await stubFor(channel).fetch(`${ORIGIN}/v1/${op}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ channel, ...body }),
          signal: controller.signal,
        });
        const text = await response.text();
        let parsed: unknown;
        try {
          parsed = JSON.parse(text);
        } catch {
          throw unavailable(
            `the channel object answered ${response.status} with a body that is not JSON`,
          );
        }
        if (response.status === 200) {
          const result = read(parsed);
          if (result === undefined)
            throw unavailable("the channel object answered an unexpected shape");
          return result;
        }
        const refusal = isRecord(parsed) && isRecord(parsed["error"]) ? parsed["error"] : undefined;
        const code = refusal?.["code"];
        if (typeof code === "string" && PASS.has(code))
          throw new RealtimeError(code as ErrorCode, String(refusal?.["message"] ?? code));
        throw unavailable(`the channel object answered ${response.status}`);
      } catch (error) {
        if (closed) throw new RealtimeError("closed", "the adapter is closed", { cause: error });
        if (error instanceof RealtimeError && error.code !== "unavailable") throw error;
        if (safe && attempt < retries) {
          await delay(Math.min(50 * 3 ** attempt, 1000));
          continue;
        }
        throw error instanceof RealtimeError
          ? error
          : unavailable("the channel object is unreachable", error);
      } finally {
        clearTimeout(timer);
        inflight.delete(controller);
      }
    }
  }

  const host: LinkHost = {
    reconnectMinMs: minDelayMs,
    reconnectMaxMs: Math.max(minDelayMs, maxDelayMs),
    keepAliveMs,
    connectTimeoutMs,
    maxQueued,
    now: Date.now,
    random: Math.random,
    async open(channel, from) {
      const query = `c=${encodeURIComponent(channel)}${from === undefined ? "" : `&after=${from}`}`;
      const response = await stubFor(channel).fetch(`${ORIGIN}/v1/ws?${query}`, {
        headers: { upgrade: "websocket" },
      });
      const socket = response.webSocket;
      if (response.status !== 101 || socket === undefined || socket === null)
        throw unavailable(`the channel object refused the subscribe socket (${response.status})`);
      return socket;
    },
    after,
    every,
    report: (error, where, channel) => report(error, { where, channel }),
    forget(link) {
      if (links.get(link.channel) === link) links.delete(link.channel);
    },
  };

  function linkFor(channel: string): ChannelLink {
    let link = links.get(channel);
    if (link === undefined) {
      link = new ChannelLink(channel, host);
      links.set(channel, link);
    }
    return link;
  }

  async function join(
    channel: string,
    add: (link: ChannelLink) => ReturnType<ChannelLink["addMessages"]>,
  ): Promise<Unsubscribe> {
    assertOpen();
    const link = linkFor(channel);
    const sub = add(link);
    try {
      await link.whenReady(connectTimeoutMs);
    } catch (error) {
      link.remove(sub);
      if (closed) throw new RealtimeError("closed", "the adapter is closed", { cause: error });
      throw error instanceof RealtimeError ? error : unavailable("could not subscribe", error);
    }
    return async () => {
      link.remove(sub);
    };
  }

  const keepRetained = (entry: StoredEntry): Record<string, unknown> => ({
    event: entry.event,
    data: entry.data,
    ...(entry.publisher === undefined ? {} : { publisher: entry.publisher }),
    ...(entry.user === undefined ? {} : { user: entry.user }),
  });

  return {
    publish(channel: string, entry: StoredEntry, rules: PublishOptions): Promise<PublishResult> {
      return call(
        channel,
        "publish",
        {
          entry: keepRetained(entry),
          options: {
            history: rules.history,
            historyTtlMs: rules.historyTtlMs,
            idempotencyTtlMs: rules.idempotencyTtlMs,
            ...(rules.key === undefined ? {} : { key: rules.key }),
          },
          ...(idleMs > 0 ? { idleMs } : {}),
        },
        readPublish,
        rules.key !== undefined,
      );
    },

    subscribe(channel: string, listener: (message: StoredMessage) => void): Promise<Unsubscribe> {
      return join(channel, (link) => link.addMessages(listener));
    },

    async history(channel: string, after: number, limit: number): Promise<HistoryResult> {
      const read = (from: number, want: number): Promise<Page> =>
        call(channel, "history", { after: from, limit: want }, readHistory, true);
      let page = await read(after, limit <= 0 ? 0 : Math.min(limit, PAGE));
      const { oldest } = page;
      const messages: StoredMessage[] = [...page.messages];
      // The object answers at most a page at a time; keep reading until `limit` or the end of the retained messages.
      while (page.more && messages.length < limit) {
        const tail = messages[messages.length - 1];
        if (tail === undefined) break;
        page = await read(tail.seq, Math.min(limit - messages.length, PAGE));
        messages.push(...page.messages);
      }
      return { messages, head: page.head, oldest };
    },

    async presenceSet(channel: string, member: Member, ttlMs: number): Promise<void> {
      await call(channel, "presence-set", { member, ttlMs, graceMs }, readEmpty, true);
    },

    presenceRemove(channel: string, id: string): Promise<boolean> {
      return call(channel, "presence-remove", { id }, readRemoved, false);
    },

    presenceList(channel: string): Promise<readonly Member[]> {
      return call(channel, "presence-list", {}, readMembers, true);
    },

    async presenceTouch(
      entries: readonly PresenceRef[],
      ttlMs: number,
    ): Promise<readonly PresenceRef[]> {
      const byChannel = new Map<string, string[]>();
      for (const entry of entries) {
        const ids = byChannel.get(entry.channel);
        if (ids === undefined) byChannel.set(entry.channel, [entry.id]);
        else ids.push(entry.id);
      }
      const results = await Promise.all(
        [...byChannel].map(async ([channel, ids]) => {
          const missing = await call(channel, "presence-touch", { ids, ttlMs }, readMissing, true);
          return missing.map((id): PresenceRef => ({ channel, id }));
        }),
      );
      return results.flat();
    },

    presenceReap(channel: string): Promise<readonly Member[]> {
      return call(channel, "presence-reap", {}, readMembers, false);
    },

    presenceSubscribe(
      channel: string,
      listener: (event: PresenceEvent) => void,
    ): Promise<Unsubscribe> {
      return join(channel, (link) => link.addPresence(listener));
    },

    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      for (const controller of inflight) controller.abort();
      for (const stop of [...sleepers]) stop(new RealtimeError("closed", "the adapter is closed"));
      for (const link of [...links.values()])
        link.dispose(new RealtimeError("closed", "the adapter is closed"));
      links.clear();
      for (const cancel of [...cancels]) cancel();
    },
  };
}

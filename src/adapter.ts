import type { Member, PresenceEvent, StoredEntry, StoredMessage } from "./protocol";

/** How a publish is retained and deduplicated. @since 1.0.0 */
export interface PublishOptions {
  /** Messages kept per channel (0 keeps none). */
  readonly history: number;
  /** Milliseconds a message is kept. */
  readonly historyTtlMs: number;
  /** An idempotency key: a second publish with it inside `idempotencyTtlMs` returns the first seq. */
  readonly key?: string;
  /** Milliseconds a key is remembered. */
  readonly idempotencyTtlMs: number;
}

/** What a publish returns. @since 1.0.0 */
export interface PublishResult {
  readonly seq: number;
  /** True when the idempotency key was already accepted and nothing was stored. */
  readonly duplicate: boolean;
}

/** What the history of a channel holds. @since 1.0.0 */
export interface HistoryResult {
  /** Retained messages with `seq > after`, in order, at most `limit`. */
  readonly messages: readonly StoredMessage[];
  /** The channel's head seq (0 for a channel that never had a message). */
  readonly head: number;
  /** The oldest retained seq (0 when nothing is retained). */
  readonly oldest: number;
}

/** A member of a channel's presence set, by id. @since 1.0.0 */
export interface PresenceRef {
  readonly channel: string;
  readonly id: string;
}

/** A way to stop listening. @since 1.0.0 */
export type Unsubscribe = () => Promise<void>;

/**
 * Storage and fan-out between servers. The adapter rules are in the realtime protocol reference (https://realtime.shadowjs.net/guides/realtime-protocol/); `adapterConformance` from
 * `@shadow-js/realtime/testing` checks an implementation against it.
 *
 * @example
 * import { memoryAdapter } from "@shadow-js/realtime/server";
 *
 * const adapter = memoryAdapter();
 * const { seq } = await adapter.publish(
 *   "chat:lobby",
 *   { event: "message", data: '"hi"' },
 *   { history: 100, historyTtlMs: 60_000, idempotencyTtlMs: 60_000 },
 * );
 * seq; // 1
 * @since 1.0.0
 */
export interface Adapter {
  /** Assigns the next seq, retains the message, delivers it to every subscriber on every server. */
  publish(channel: string, entry: StoredEntry, options: PublishOptions): Promise<PublishResult>;
  /** Listens to a channel's live messages (in increasing seq order). */
  subscribe(channel: string, listener: (message: StoredMessage) => void): Promise<Unsubscribe>;
  /** Reads retained messages after a seq. */
  history(channel: string, after: number, limit: number): Promise<HistoryResult>;
  /** Adds or updates a presence member, with an expiry. Emits `join` for a new member and `set` for a newer version. */
  presenceSet(channel: string, member: Member, ttlMs: number): Promise<void>;
  /** Removes a member. Resolves true (and emits `leave`) only for the call that removed it. */
  presenceRemove(channel: string, id: string): Promise<boolean>;
  /** The live members of a channel. */
  presenceList(channel: string): Promise<readonly Member[]>;
  /** Refreshes the expiry of members; returns the entries that no longer exist (the caller sets them again). */
  presenceTouch(entries: readonly PresenceRef[], ttlMs: number): Promise<readonly PresenceRef[]>;
  /** Removes the expired members of a channel, emitting one `leave` for each; returns them. */
  presenceReap(channel: string): Promise<readonly Member[]>;
  /** Listens to a channel's presence changes. */
  presenceSubscribe(
    channel: string,
    listener: (event: PresenceEvent) => void,
  ): Promise<Unsubscribe>;
  /** Releases connections and timers. */
  close(): Promise<void>;
}

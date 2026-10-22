import type {
  Adapter,
  HistoryResult,
  PresenceRef,
  PublishOptions,
  PublishResult,
  Unsubscribe,
} from "./adapter";
import type { Member, PresenceEvent, StoredEntry, StoredMessage } from "./protocol";

interface ChannelState {
  seq: number;
  log: StoredMessage[];
  readonly keys: Map<string, { readonly seq: number; readonly expires: number }>;
  readonly listeners: Set<(message: StoredMessage) => void>;
  readonly pending: StoredMessage[];
  draining: boolean;
  lastActive: number;
}

interface PresenceEntry {
  member: Member;
  expires: number;
}

/**
 * Options of {@link memoryAdapter}.
 * @since 1.0.0
 */
export interface MemoryAdapterOptions {
  /** The clock, in epoch milliseconds. Default: `Date.now`. */
  readonly now?: () => number;
  /**
   * Milliseconds a channel with no listener and no retained message is remembered. After that its head is forgotten and its
   * numbering restarts, which a resuming client sees as `reset`. Default: one hour.
   */
  readonly idleMs?: number;
}

/**
 * The single-process adapter, and the default of `createHub`. Messages, history, idempotency keys and presence live in memory
 * of this process; several hubs created over one adapter object share them, which is how tests exercise two servers. Use the Redis
 * or Cloudflare adapter when more than one process serves clients.
 *
 * @param options The clock.
 * @returns The adapter.
 * @example
 * import { createHub, memoryAdapter } from "@shadow-js/realtime/server";
 *
 * const adapter = memoryAdapter();
 * const hub = createHub({ adapter });
 * void hub.close();
 * @since 1.0.0
 */
export function memoryAdapter(options: MemoryAdapterOptions = {}): Adapter {
  const now = options.now ?? Date.now;
  const idleMs = options.idleMs ?? 3_600_000;
  let sweepAt = 1024;
  const channels = new Map<string, ChannelState>();
  const presence = new Map<string, Map<string, PresenceEntry>>();
  const presenceListeners = new Map<string, Set<(event: PresenceEvent) => void>>();
  const presenceQueue: PresenceEvent[] = [];
  let presenceDraining = false;
  let closed = false;

  /** Forgets channels nobody listens to and nothing retains; runs when the table has doubled since the last sweep. */
  function sweep(): void {
    const cutoff = now() - idleMs;
    for (const [name, state] of channels)
      if (state.listeners.size === 0 && state.lastActive < cutoff) {
        trim(state, state.log.length, idleMs);
        if (state.log.length === 0) channels.delete(name);
      }
    sweepAt = Math.max(1024, channels.size * 2);
  }

  function channelOf(name: string): ChannelState {
    let state = channels.get(name);
    if (state === undefined) {
      if (channels.size >= sweepAt) sweep();
      state = {
        lastActive: now(),
        seq: 0,
        log: [],
        keys: new Map(),
        listeners: new Set(),
        pending: [],
        draining: false,
      };
      channels.set(name, state);
    }
    state.lastActive = now();
    return state;
  }

  /** Delivers queued messages in order; a listener that publishes in turn queues behind the message being delivered. */
  function drain(state: ChannelState): void {
    if (state.draining) return;
    state.draining = true;
    try {
      for (let next = state.pending.shift(); next !== undefined; next = state.pending.shift()) {
        for (const listener of [...state.listeners]) {
          try {
            listener(next);
          } catch {
            // A listener's failure is its own: delivery to the others goes on (guarantee G13).
          }
        }
      }
    } finally {
      state.draining = false;
    }
  }

  function emitPresence(event: PresenceEvent): void {
    presenceQueue.push(event);
    if (presenceDraining) return;
    presenceDraining = true;
    try {
      for (let next = presenceQueue.shift(); next !== undefined; next = presenceQueue.shift()) {
        for (const listener of [...(presenceListeners.get(next.channel) ?? [])]) {
          try {
            listener(next);
          } catch {
            // See `drain`.
          }
        }
      }
    } finally {
      presenceDraining = false;
    }
  }

  function trim(state: ChannelState, history: number, ttl: number): void {
    const cutoff = now() - ttl;
    let drop = Math.max(0, state.log.length - history);
    while (drop < state.log.length && (state.log[drop]?.at ?? Infinity) < cutoff) drop += 1;
    if (drop > 0) state.log = state.log.slice(drop);
  }

  function live(name: string): Map<string, PresenceEntry> {
    let set = presence.get(name);
    if (set === undefined) {
      set = new Map();
      presence.set(name, set);
    }
    return set;
  }

  function check(): void {
    if (closed) throw new Error("the adapter is closed.");
  }

  return {
    async publish(
      channel: string,
      entry: StoredEntry,
      publish: PublishOptions,
    ): Promise<PublishResult> {
      check();
      const state = channelOf(channel);
      const at = now();
      if (publish.key !== undefined) {
        const known = state.keys.get(publish.key);
        if (known !== undefined && known.expires > at) return { seq: known.seq, duplicate: true };
        // Expired keys are swept when a new one is added, so the map stays bounded by the rate of publishes.
        if (state.keys.size >= 4096)
          for (const [key, value] of state.keys) if (value.expires <= at) state.keys.delete(key);
      }
      state.seq += 1;
      const message: StoredMessage = { ...entry, seq: state.seq, at };
      if (publish.key !== undefined)
        state.keys.set(publish.key, {
          seq: state.seq,
          expires: at + publish.idempotencyTtlMs,
        });
      if (publish.history > 0) {
        state.log.push(message);
        trim(state, publish.history, publish.historyTtlMs);
      } else {
        state.log = [];
      }
      state.pending.push(message);
      drain(state);
      return { seq: message.seq, duplicate: false };
    },

    async subscribe(
      channel: string,
      listener: (message: StoredMessage) => void,
    ): Promise<Unsubscribe> {
      check();
      const state = channelOf(channel);
      state.listeners.add(listener);
      return async () => {
        state.listeners.delete(listener);
      };
    },

    async history(channel: string, after: number, limit: number): Promise<HistoryResult> {
      check();
      const state = channels.get(channel);
      if (state === undefined) return { messages: [], head: 0, oldest: 0 };
      const retained = state.log;
      const messages: StoredMessage[] = [];
      for (const message of retained) {
        if (messages.length >= limit) break;
        if (message.seq > after) messages.push(message);
      }
      return { messages, head: state.seq, oldest: retained[0]?.seq ?? 0 };
    },

    async presenceSet(channel: string, member: Member, ttlMs: number): Promise<void> {
      check();
      const set = live(channel);
      const known = set.get(member.id);
      if (known !== undefined && known.member.v > member.v) return;
      set.set(member.id, { member, expires: now() + ttlMs });
      if (known === undefined) emitPresence({ channel, op: "join", member });
      else if (known.member.v < member.v) emitPresence({ channel, op: "set", member });
    },

    async presenceRemove(channel: string, id: string): Promise<boolean> {
      check();
      const set = presence.get(channel);
      const known = set?.get(id);
      if (set === undefined || known === undefined) return false;
      set.delete(id);
      if (set.size === 0) presence.delete(channel);
      emitPresence({ channel, op: "leave", member: known.member });
      return true;
    },

    async presenceList(channel: string): Promise<readonly Member[]> {
      check();
      const at = now();
      const out: Member[] = [];
      for (const entry of presence.get(channel)?.values() ?? [])
        if (entry.expires > at) out.push(entry.member);
      return out;
    },

    async presenceTouch(entries, ttlMs): Promise<readonly PresenceRef[]> {
      check();
      const expires = now() + ttlMs;
      const missing: PresenceRef[] = [];
      for (const ref of entries) {
        const entry = presence.get(ref.channel)?.get(ref.id);
        if (entry === undefined) missing.push(ref);
        else entry.expires = expires;
      }
      return missing;
    },

    async presenceReap(channel: string): Promise<readonly Member[]> {
      check();
      const set = presence.get(channel);
      if (set === undefined) return [];
      const at = now();
      const removed: Member[] = [];
      for (const [id, entry] of [...set]) {
        if (entry.expires > at) continue;
        set.delete(id);
        removed.push(entry.member);
        emitPresence({ channel, op: "leave", member: entry.member });
      }
      if (set.size === 0) presence.delete(channel);
      return removed;
    },

    async presenceSubscribe(
      channel: string,
      listener: (event: PresenceEvent) => void,
    ): Promise<Unsubscribe> {
      check();
      let set = presenceListeners.get(channel);
      if (set === undefined) {
        set = new Set();
        presenceListeners.set(channel, set);
      }
      set.add(listener);
      const owner = set;
      return async () => {
        owner.delete(listener);
        if (owner.size === 0 && presenceListeners.get(channel) === owner)
          presenceListeners.delete(channel);
      };
    },

    async close(): Promise<void> {
      closed = true;
      channels.clear();
      presence.clear();
      presenceListeners.clear();
    },
  };
}

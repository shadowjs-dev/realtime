/**
 * The logic of one channel, free of the Workers runtime: it takes the storage, the clock and the sockets as plain objects. The
 * Durable Object class wires it to the platform; the unit tests wire it to an in-memory storage. Everything that touches state
 * runs one at a time (a queue in front of every operation), so numbering is gapless by construction and a presence member
 * leaves exactly once, whether the object is awake, evicted and restarted, or woken by its alarm.
 *
 * Storage layout (keys sort as strings, numbers are zero padded):
 * - `meta`: head seq, oldest retained seq and its time, the retention of the last publish, what is due and when;
 * - `h:<seq>`: one retained message per key (a value stays far below the platform's limit);
 * - `p:<id>`: a presence member and its expiry;
 * - `k:<key>`: an idempotency key and the seq it produced; `x:<expires>:<key>`: the expiry index used to sweep keys.
 * @packageDocumentation
 */
import type { PublishOptions, PublishResult } from "../adapter";
import { RealtimeError } from "../errors";
import type { Member, PresenceEvent, StoredEntry, StoredMessage } from "../protocol";
import type { ChannelSocket, ChannelStorage } from "./types";
import { type HistoryPage, historyKey, type LinkFrame, MAX_MEMBERS, seqOfKey } from "./wire";

const META = "meta";
const MESSAGES = "h:";
const MEMBERS = "p:";
const KEYS = "k:";
const KEY_INDEX = "x:";
/** `x:` + 16 digits + `:`; what follows is the idempotency key. */
const KEY_INDEX_PREFIX = 19;
/** The platform accepts at most 128 keys in one `delete`, `put` or `get`. */
const BATCH = 128;
/** The most messages one operation deletes; a larger backlog is finished by the next operation or alarm. */
const TRIM_MAX = 1024;
/** Waiting this long after the first idempotency key expires lets one alarm sweep many. */
const SWEEP_SLACK_MS = 5000;
/** The same for retained messages. */
const HISTORY_SLACK_MS = 1000;
const PAGE_MESSAGES = 512;
const PAGE_BYTES = 4 * 1024 * 1024;
const REPLAY_MESSAGES = 10_000;
const REPLAY_BYTES = 8 * 1024 * 1024;

interface Meta {
  v: 1;
  channel: string;
  /** The highest seq assigned. */
  head: number;
  /** The oldest retained seq, 0 when nothing is retained. */
  oldest: number;
  /** When the oldest retained message was accepted. */
  oldestAt: number;
  /** The `history` count of the last publish. */
  keep: number;
  /** The `historyTtlMs` of the last publish. */
  ttlMs: number;
  /** When the earliest idempotency key can be swept, 0 for none. */
  keysDue: number;
  /** When the earliest presence member can be reaped by the alarm, 0 for none. */
  presenceDue: number;
  /** How long after its expiry the alarm reaps a member; the hub's own `presenceReap` runs inside that window. */
  graceMs: number;
  /** Forget the channel after this long without activity, 0 to keep it for good. */
  idleMs: number;
  /** The time of the last publish or presence change. */
  touched: number;
}

interface KeyRecord {
  readonly seq: number;
  readonly expires: number;
}

interface MemberRecord {
  readonly member: Member;
  expires: number;
}

/** A member in memory: `stored` is the expiry the storage holds, which lags behind `expires` to save writes. */
interface MemberState {
  member: Member;
  expires: number;
  stored: number;
}

/** What {@link ChannelCore} needs from its surroundings. @internal */
export interface CoreOptions {
  readonly storage: ChannelStorage;
  /** The clock in epoch milliseconds. Default: `Date.now`. */
  readonly now?: () => number;
  /** The most presence members one channel keeps. Default: {@link MAX_MEMBERS}. */
  readonly maxMembers?: number;
}

function pad(value: number): string {
  return String(value).padStart(16, "0");
}

function chunks<T>(items: readonly T[]): T[][] {
  const out: T[][] = [];
  for (let at = 0; at < items.length; at += BATCH) out.push(items.slice(at, at + BATCH));
  return out;
}

function sizeOf(message: StoredMessage): number {
  return message.data.length + message.event.length + 96;
}

/**
 * The single writer of one channel.
 * @internal
 */
export class ChannelCore {
  readonly #storage: ChannelStorage;
  readonly #now: () => number;
  readonly #maxMembers: number;
  readonly #sockets = new Set<ChannelSocket>();
  #tail: Promise<void> = Promise.resolve();
  #meta: Meta | undefined;
  #members: Map<string, MemberState> | undefined;
  /** The alarm time the storage holds: `undefined` unknown, `null` none. */
  #alarmAt: number | null | undefined;

  constructor(options: CoreOptions) {
    this.#storage = options.storage;
    this.#now = options.now ?? Date.now;
    this.#maxMembers = options.maxMembers ?? MAX_MEMBERS;
  }

  /** Registers sockets that were open before this instance was created (the runtime kept them while the object slept). */
  adopt(sockets: Iterable<ChannelSocket>): void {
    for (const socket of sockets) this.#sockets.add(socket);
  }

  /** Forgets a socket that closed. */
  detach(socket: ChannelSocket): void {
    this.#sockets.delete(socket);
  }

  /** The number of sockets attached. */
  get sockets(): number {
    return this.#sockets.size;
  }

  // --- the queue -----------------------------------------------------------------------------------------------------

  /** Runs `task` after every earlier task finished. A failure drops the caches, so the next task reads the storage afresh. */
  #run<T>(task: () => Promise<T>): Promise<T> {
    const result = this.#tail.then(task).catch((error: unknown) => {
      if (!(error instanceof RealtimeError)) {
        this.#meta = undefined;
        this.#members = undefined;
        this.#alarmAt = undefined;
      }
      throw error;
    });
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  // --- state ---------------------------------------------------------------------------------------------------------

  async #metaFor(channel: string): Promise<Meta> {
    const meta = await this.#storedMeta();
    if (meta !== undefined) {
      if (meta.channel !== channel)
        throw new RealtimeError("conflict", "the object belongs to another channel");
      return meta;
    }
    const fresh: Meta = {
      v: 1,
      channel,
      head: 0,
      oldest: 0,
      oldestAt: 0,
      keep: 0,
      ttlMs: 0,
      keysDue: 0,
      presenceDue: 0,
      graceMs: 0,
      idleMs: 0,
      touched: this.#now(),
    };
    this.#meta = fresh;
    return fresh;
  }

  async #storedMeta(): Promise<Meta | undefined> {
    if (this.#meta === undefined) this.#meta = await this.#storage.get<Meta>(META);
    return this.#meta;
  }

  async #loadMembers(): Promise<Map<string, MemberState>> {
    if (this.#members === undefined) {
      const rows = await this.#storage.list<MemberRecord>({ prefix: MEMBERS });
      const members = new Map<string, MemberState>();
      for (const [key, record] of rows)
        members.set(key.slice(MEMBERS.length), {
          member: record.member,
          expires: record.expires,
          stored: record.expires,
        });
      this.#members = members;
    }
    return this.#members;
  }

  /** Writes `puts` and `deletes` and the metadata in one transaction. */
  async #commit(
    meta: Meta,
    puts: Readonly<Record<string, unknown>>,
    deletes: readonly string[],
  ): Promise<void> {
    await this.#storage.transaction(async (transaction) => {
      await transaction.put({ ...puts, [META]: meta });
      for (const batch of chunks(deletes)) await transaction.delete(batch);
    });
    this.#meta = meta;
  }

  // --- fan-out -------------------------------------------------------------------------------------------------------

  #send(socket: ChannelSocket, text: string): boolean {
    try {
      socket.send(text);
      return true;
    } catch {
      this.#sockets.delete(socket);
      try {
        socket.close(1011, "write failed");
      } catch {
        // The socket is gone already.
      }
      return false;
    }
  }

  #broadcast(frame: LinkFrame): void {
    if (this.#sockets.size === 0) return;
    const text = JSON.stringify(frame);
    for (const socket of [...this.#sockets]) this.#send(socket, text);
  }

  #emit(op: PresenceEvent["op"], member: Member): void {
    this.#broadcast({ t: "pres", op, member });
  }

  // --- messages ------------------------------------------------------------------------------------------------------

  /**
   * Assigns the next seq, retains the message, remembers the key and sends the message to every attached socket.
   *
   * @param channel The channel name.
   * @param entry The message.
   * @param options Retention and the idempotency key.
   * @param idleMs Forget the channel after this long without activity (0 keeps it).
   * @returns The seq and whether the key was already accepted.
   */
  publish(
    channel: string,
    entry: StoredEntry,
    options: PublishOptions,
    idleMs = 0,
  ): Promise<PublishResult> {
    return this.#run(async () => {
      const meta = await this.#metaFor(channel);
      const now = this.#now();
      if (options.key !== undefined) {
        const known = await this.#storage.get<KeyRecord>(KEYS + options.key);
        if (known !== undefined && known.expires > now) return { seq: known.seq, duplicate: true };
      }
      const seq = meta.head + 1;
      const message: StoredMessage = {
        seq,
        event: entry.event,
        data: entry.data,
        at: now,
        ...(entry.publisher === undefined ? {} : { publisher: entry.publisher }),
        ...(entry.user === undefined ? {} : { user: entry.user }),
      };
      const plan = await this.#planTrim(meta, now, options.history, options.historyTtlMs, 1);
      const next: Meta = {
        ...meta,
        head: seq,
        keep: options.history,
        ttlMs: options.historyTtlMs,
        idleMs,
        touched: now,
        oldest: plan.oldest,
        oldestAt: plan.oldestAt,
      };
      const puts: Record<string, unknown> = {};
      if (options.history > 0) {
        puts[historyKey(seq)] = message;
        if (next.oldest === 0) {
          next.oldest = seq;
          next.oldestAt = now;
        }
      }
      if (options.key !== undefined) {
        const expires = now + options.idempotencyTtlMs;
        puts[KEYS + options.key] = { seq, expires } satisfies KeyRecord;
        puts[`${KEY_INDEX}${pad(expires)}:${options.key}`] = 1;
        const due = expires + SWEEP_SLACK_MS;
        next.keysDue = next.keysDue === 0 ? due : Math.min(next.keysDue, due);
      }
      await this.#commit(next, puts, plan.deletes);
      this.#broadcast({ t: "msg", m: message });
      await this.#schedule(next);
      return { seq, duplicate: false };
    });
  }

  /**
   * Works out which retained messages to delete so that, after `extra` more messages are stored, at most `keep` remain and
   * none is older than `ttlMs`. It reads at most a few pages and deletes at most {@link TRIM_MAX} messages.
   */
  async #planTrim(
    meta: Meta,
    now: number,
    keep: number,
    ttlMs: number,
    extra: number,
  ): Promise<{ deletes: string[]; oldest: number; oldestAt: number }> {
    const deletes: string[] = [];
    let oldest = meta.oldest;
    let oldestAt = meta.oldestAt;
    if (oldest === 0) return { deletes, oldest: 0, oldestAt: 0 };
    const target = meta.head + 1 + extra - keep;
    if (oldest < target) {
      const stop = Math.min(target, meta.head + 1, oldest + TRIM_MAX);
      for (let seq = oldest; seq < stop; seq += 1) deletes.push(historyKey(seq));
      oldest = stop;
      if (oldest > meta.head) return { deletes, oldest: 0, oldestAt: 0 };
      const first = await this.#firstFrom(oldest);
      if (first === undefined) return { deletes, oldest: 0, oldestAt: 0 };
      oldest = first.seq;
      oldestAt = first.at;
    }
    const cutoff = now - ttlMs;
    while (oldestAt < cutoff && deletes.length < TRIM_MAX) {
      const page = await this.#storage.list<StoredMessage>({
        prefix: MESSAGES,
        start: historyKey(oldest),
        limit: BATCH,
      });
      let survivor: StoredMessage | undefined;
      for (const message of page.values()) {
        if (message.at >= cutoff || deletes.length >= TRIM_MAX) {
          survivor = message;
          break;
        }
        deletes.push(historyKey(message.seq));
      }
      if (survivor !== undefined) {
        oldest = survivor.seq;
        oldestAt = survivor.at;
        break;
      }
      if (page.size < BATCH) return { deletes, oldest: 0, oldestAt: 0 };
      const last = deletes.length === 0 ? undefined : deletes[deletes.length - 1];
      if (last === undefined) break;
      oldest = seqOfKey(last) + 1;
      const next = await this.#firstFrom(oldest);
      if (next === undefined) return { deletes, oldest: 0, oldestAt: 0 };
      oldest = next.seq;
      oldestAt = next.at;
    }
    return { deletes, oldest, oldestAt };
  }

  async #firstFrom(seq: number): Promise<StoredMessage | undefined> {
    const page = await this.#storage.list<StoredMessage>({
      prefix: MESSAGES,
      start: historyKey(seq),
      limit: 1,
    });
    for (const message of page.values()) return message;
    return undefined;
  }

  /** Whether retained messages are past their time or above the count of the last publish. */
  #retentionDue(meta: Meta, now: number): boolean {
    if (meta.oldest === 0) return false;
    return meta.head - meta.oldest + 1 > meta.keep || meta.oldestAt + meta.ttlMs < now;
  }

  async #retain(meta: Meta, now: number): Promise<boolean> {
    if (!this.#retentionDue(meta, now)) return false;
    const plan = await this.#planTrim(meta, now, meta.keep, meta.ttlMs, 0);
    if (plan.deletes.length === 0 && plan.oldest === meta.oldest) return false;
    await this.#commit({ ...meta, oldest: plan.oldest, oldestAt: plan.oldestAt }, {}, plan.deletes);
    return true;
  }

  /**
   * Reads retained messages after a seq.
   *
   * @param channel The channel name.
   * @param after The last seq the caller holds.
   * @param limit The most messages to return (0 asks for the head only).
   * @returns A page; `more` says retained messages remain after it.
   */
  history(channel: string, after: number, limit: number): Promise<HistoryPage> {
    return this.#run(async () => {
      let meta = await this.#metaFor(channel);
      if (await this.#retain(meta, this.#now())) meta = this.#meta ?? meta;
      if (limit <= 0 || after >= meta.head || meta.oldest === 0)
        return { messages: [], head: meta.head, oldest: meta.oldest, more: false };
      const cap = Math.min(limit, PAGE_MESSAGES);
      const rows = await this.#storage.list<StoredMessage>({
        prefix: MESSAGES,
        start: historyKey(Math.max(after + 1, meta.oldest)),
        limit: cap,
      });
      const messages: StoredMessage[] = [];
      let bytes = 0;
      for (const message of rows.values()) {
        if (messages.length > 0 && bytes + sizeOf(message) > PAGE_BYTES) break;
        messages.push(message);
        bytes += sizeOf(message);
      }
      const last = messages[messages.length - 1];
      // Retained messages are contiguous, so any seq above the last one returned (up to the head) is retained too.
      return {
        messages,
        head: meta.head,
        oldest: meta.oldest,
        more: last !== undefined && last.seq < meta.head,
      };
    });
  }

  // --- subscribers ---------------------------------------------------------------------------------------------------

  /**
   * Starts sending a socket the channel's messages and presence events: the head, the live members, the messages after
   * `after` that are retained, then every later event. Nothing can interleave, because it runs in the queue.
   *
   * @param socket The server end of the subscribe socket.
   * @param channel The channel name.
   * @param after The last seq the subscriber holds, when it is resuming.
   */
  attach(socket: ChannelSocket, channel: string, after: number | undefined): Promise<void> {
    return this.#run(async () => {
      let meta = await this.#metaFor(channel);
      const now = this.#now();
      if (await this.#retain(meta, now)) meta = this.#meta ?? meta;
      const members = await this.#loadMembers();
      const live: Member[] = [];
      for (const state of members.values()) if (state.expires > now) live.push(state.member);
      const frames: string[] = [
        JSON.stringify({ t: "ready", head: meta.head, oldest: meta.oldest } satisfies LinkFrame),
        JSON.stringify({ t: "snap", members: live } satisfies LinkFrame),
      ];
      let truncated = false;
      if (after !== undefined && after < meta.head && meta.oldest > 0) {
        let cursor = Math.max(after, meta.oldest - 1);
        let count = 0;
        let bytes = 0;
        replay: while (cursor < meta.head) {
          const rows = await this.#storage.list<StoredMessage>({
            prefix: MESSAGES,
            start: historyKey(cursor + 1),
            limit: BATCH,
          });
          if (rows.size === 0) break;
          for (const message of rows.values()) {
            frames.push(JSON.stringify({ t: "msg", m: message } satisfies LinkFrame));
            cursor = message.seq;
            count += 1;
            bytes += sizeOf(message);
            if (count >= REPLAY_MESSAGES || bytes >= REPLAY_BYTES) {
              truncated = cursor < meta.head;
              break replay;
            }
          }
          if (rows.size < BATCH) break;
        }
      }
      if (truncated) frames.push(JSON.stringify({ t: "more" } satisfies LinkFrame));
      for (const frame of frames) {
        try {
          socket.send(frame);
        } catch {
          return;
        }
      }
      this.#sockets.add(socket);
    });
  }

  // --- presence ------------------------------------------------------------------------------------------------------

  /**
   * Adds or updates a member. An older version is ignored; the first sighting is a `join`, a newer version a `set`.
   *
   * @param channel The channel name.
   * @param member The member.
   * @param ttlMs Milliseconds until it expires without a touch.
   * @param graceMs Milliseconds after expiry that the alarm waits before it reaps (so the hub's own reap comes first).
   */
  presenceSet(channel: string, member: Member, ttlMs: number, graceMs: number): Promise<void> {
    return this.#run(async () => {
      const meta = await this.#metaFor(channel);
      const members = await this.#loadMembers();
      const known = members.get(member.id);
      if (known !== undefined && known.member.v > member.v) return;
      if (known === undefined && members.size >= this.#maxMembers)
        throw new RealtimeError("limit", "the channel has too many presence members");
      const now = this.#now();
      const expires = now + ttlMs;
      const changed = known === undefined || known.member.v < member.v;
      const due = expires + graceMs;
      const next: Meta = {
        ...meta,
        graceMs,
        touched: now,
        presenceDue: meta.presenceDue === 0 ? due : Math.min(meta.presenceDue, due),
      };
      const persist = changed || known === undefined || expires - known.stored >= ttlMs / 2;
      if (persist || next.presenceDue !== meta.presenceDue || next.graceMs !== meta.graceMs) {
        await this.#commit(
          next,
          persist ? { [MEMBERS + member.id]: { member, expires } satisfies MemberRecord } : {},
          [],
        );
      }
      members.set(member.id, {
        member,
        expires,
        stored: persist ? expires : (known?.stored ?? expires),
      });
      if (known === undefined) this.#emit("join", member);
      else if (known.member.v < member.v) this.#emit("set", member);
      await this.#schedule(next);
    });
  }

  /**
   * Removes a member.
   *
   * @param channel The channel name.
   * @param id The member id.
   * @returns True when the member existed (and a `leave` was sent); false otherwise.
   */
  presenceRemove(channel: string, id: string): Promise<boolean> {
    return this.#run(async () => {
      const meta = await this.#metaFor(channel);
      const members = await this.#loadMembers();
      const known = members.get(id);
      if (known === undefined) return false;
      const next: Meta = members.size === 1 ? { ...meta, presenceDue: 0 } : meta;
      await this.#commit(next, {}, [MEMBERS + id]);
      members.delete(id);
      this.#emit("leave", known.member);
      await this.#schedule(next);
      return true;
    });
  }

  /**
   * The members that have not expired.
   *
   * @param channel The channel name.
   * @returns The members.
   */
  presenceList(channel: string): Promise<Member[]> {
    return this.#run(async () => {
      await this.#metaFor(channel);
      const members = await this.#loadMembers();
      const now = this.#now();
      const out: Member[] = [];
      for (const state of members.values()) if (state.expires > now) out.push(state.member);
      return out;
    });
  }

  /**
   * Extends the expiry of members. The storage is written only when the new expiry is at least half a time to live beyond
   * the stored one, so a heartbeat every few seconds costs a write every few beats; a restart in between loses at most
   * that half, which a heartbeat repairs long before the member would expire.
   *
   * @param channel The channel name.
   * @param ids The member ids.
   * @param ttlMs Milliseconds until they expire without another touch.
   * @returns The ids that no longer exist.
   */
  presenceTouch(channel: string, ids: readonly string[], ttlMs: number): Promise<string[]> {
    return this.#run(async () => {
      const meta = await this.#metaFor(channel);
      const members = await this.#loadMembers();
      const now = this.#now();
      const expires = now + ttlMs;
      const missing: string[] = [];
      const puts: Record<string, unknown> = {};
      const written: MemberState[] = [];
      let touched = 0;
      for (const id of ids) {
        const state = members.get(id);
        if (state === undefined) {
          missing.push(id);
          continue;
        }
        // Like the reference adapter, the expiry becomes exactly now + ttl: a touch with a shorter life shortens it.
        state.expires = expires;
        touched += 1;
        if (state.expires - state.stored >= ttlMs / 2) {
          puts[MEMBERS + id] = {
            member: state.member,
            expires: state.expires,
          } satisfies MemberRecord;
          written.push(state);
        }
      }
      const keys = Object.keys(puts);
      // `put` takes at most 128 entries.
      for (const batch of chunks(keys)) {
        const part: Record<string, unknown> = {};
        for (const key of batch) part[key] = puts[key];
        await this.#storage.put(part);
      }
      for (const state of written) state.stored = state.expires;
      // Touches usually push the expiry later, which the alarm finds out for itself; a shorter life needs an earlier alarm.
      const due = expires + meta.graceMs;
      if (touched > 0 && (meta.presenceDue === 0 || due < meta.presenceDue)) {
        const next: Meta = { ...meta, presenceDue: due };
        await this.#commit(next, {}, []);
        await this.#schedule(next);
      }
      return missing;
    });
  }

  /**
   * Removes every expired member and sends one `leave` for each.
   *
   * @param channel The channel name.
   * @returns The members removed.
   */
  presenceReap(channel: string): Promise<Member[]> {
    return this.#run(async () => {
      const meta = await this.#metaFor(channel);
      return this.#reap(meta, this.#now());
    });
  }

  async #reap(meta: Meta, now: number): Promise<Member[]> {
    const members = await this.#loadMembers();
    const gone: MemberState[] = [];
    for (const state of members.values()) if (state.expires <= now) gone.push(state);
    if (gone.length === 0) return [];
    let due = 0;
    for (const state of members.values())
      if (state.expires > now) {
        const at = state.expires + meta.graceMs;
        due = due === 0 ? at : Math.min(due, at);
      }
    const next: Meta = { ...meta, presenceDue: due };
    await this.#commit(
      next,
      {},
      gone.map((state) => MEMBERS + state.member.id),
    );
    for (const state of gone) {
      members.delete(state.member.id);
      this.#emit("leave", state.member);
    }
    return gone.map((state) => state.member);
  }

  // --- the alarm -----------------------------------------------------------------------------------------------------

  /** The time the next alarm is due, `undefined` when nothing is pending. */
  #nextDue(meta: Meta, now: number): number | undefined {
    let due = 0;
    const consider = (at: number): void => {
      if (at > 0 && (due === 0 || at < due)) due = at;
    };
    consider(meta.presenceDue);
    consider(meta.keysDue);
    if (meta.oldest > 0)
      consider(
        meta.head - meta.oldest + 1 > meta.keep
          ? now
          : meta.oldestAt + meta.ttlMs + HISTORY_SLACK_MS,
      );
    if (meta.idleMs > 0) {
      // A channel with a subscriber is not idle: look again after another idle period instead of waking at once, every time.
      const idleAt = meta.touched + meta.idleMs;
      consider(this.#sockets.size > 0 && idleAt <= now ? now + meta.idleMs : idleAt);
    }
    return due === 0 ? undefined : Math.max(due, now);
  }

  /** Makes the storage's alarm fire no later than the next thing that is due. An earlier alarm is left alone: it wakes, finds nothing, and sets the right time. */
  async #schedule(meta: Meta): Promise<void> {
    const now = this.#now();
    const due = this.#nextDue(meta, now);
    if (this.#alarmAt === undefined) this.#alarmAt = await this.#storage.getAlarm();
    if (due === undefined) {
      if (this.#alarmAt !== null) {
        await this.#storage.deleteAlarm();
        this.#alarmAt = null;
      }
      return;
    }
    if (this.#alarmAt === null || this.#alarmAt > due) {
      await this.#storage.setAlarm(due);
      this.#alarmAt = due;
    }
  }

  /** Deletes idempotency keys that expired, in batches. */
  async #sweepKeys(meta: Meta, now: number): Promise<Meta> {
    for (let pass = 0; pass < 32; pass += 1) {
      const rows = await this.#storage.list<number>({
        prefix: KEY_INDEX,
        end: KEY_INDEX + pad(now + 1),
        limit: BATCH,
      });
      if (rows.size === 0) break;
      const indexKeys = [...rows.keys()];
      const names = indexKeys.map((key) => key.slice(KEY_INDEX_PREFIX));
      const records = await this.#storage.get<KeyRecord>(names.map((name) => KEYS + name));
      const deletes = [...indexKeys];
      for (const name of names) {
        const record = records.get(KEYS + name);
        if (record === undefined || record.expires <= now) deletes.push(KEYS + name);
      }
      await this.#storage.transaction(async (transaction) => {
        for (const batch of chunks(deletes)) await transaction.delete(batch);
      });
      if (rows.size < BATCH) break;
    }
    const first = await this.#storage.list<number>({ prefix: KEY_INDEX, limit: 1 });
    let keysDue = 0;
    for (const key of first.keys())
      keysDue = Number(key.slice(KEY_INDEX.length, KEY_INDEX.length + 16)) + SWEEP_SLACK_MS;
    return { ...meta, keysDue };
  }

  /**
   * The alarm handler: reaps expired presence members (one `leave` each), sweeps expired idempotency keys, trims retained
   * messages that expired, forgets a channel that has been idle for its `idleMs`, and sets the next alarm. It is safe to run
   * more often than needed, and to run again after a failure.
   */
  alarm(): Promise<void> {
    return this.#run(async () => {
      this.#alarmAt = null;
      let meta = await this.#storedMeta();
      if (meta === undefined) return;
      const now = this.#now();
      if (meta.presenceDue > 0 && meta.presenceDue <= now) {
        await this.#reap(meta, now);
        meta = this.#meta ?? meta;
        // An alarm earlier than the earliest expiry (members were touched) reaps nothing: move the due time instead.
        let due = 0;
        for (const state of (await this.#loadMembers()).values()) {
          const at = state.expires + meta.graceMs;
          due = due === 0 ? at : Math.min(due, at);
        }
        if (due !== meta.presenceDue) {
          meta = { ...meta, presenceDue: due };
          await this.#commit(meta, {}, []);
        }
      }
      if (meta.keysDue > 0 && meta.keysDue <= now) {
        meta = await this.#sweepKeys(meta, now);
        await this.#commit(meta, {}, []);
      }
      if (await this.#retain(meta, now)) meta = this.#meta ?? meta;
      if (
        meta.idleMs > 0 &&
        meta.touched + meta.idleMs <= now &&
        meta.oldest === 0 &&
        meta.presenceDue === 0 &&
        meta.keysDue === 0 &&
        this.#sockets.size === 0
      ) {
        await this.#storage.deleteAll();
        this.#meta = undefined;
        this.#members = undefined;
        this.#alarmAt = null;
        return;
      }
      await this.#schedule(meta);
    });
  }
}

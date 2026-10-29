import type { Adapter, PublishResult, Unsubscribe } from "../adapter";
import { RealtimeError } from "../errors";
import { memoryAdapter } from "../memory";
import {
  byteLength,
  type ClientFrame,
  decodeClientFrame,
  isChannelName,
  isEventName,
  isIdempotencyKey,
  type Json,
  type Member,
  PROTOCOL_VERSION,
  type PresenceEvent,
  type StoredMessage,
} from "../protocol";
import { type HubLimits, resolveLimits, TokenBucket, WindowCounter } from "./limits";
import {
  type ChannelRule,
  type CompiledRule,
  compileRule,
  type Identity,
  matchRule,
  type RuleContext,
} from "./rules";

/**
 * What a hub needs from a connection's transport. A WebSocket, an SSE stream and an in-memory test client all fit.
 * @since 1.0.0
 */
export interface Transport {
  /** Sends one frame (text). False when the transport is closed. */
  send(frame: string): boolean;
  /** Closes the transport with a WebSocket close code and a reason of at most 123 bytes. */
  close(code: number, reason: string): void;
  /** Bytes accepted by `send` that the peer has not yet taken. When present, a connection over the limit is closed. */
  buffered?(): number;
}

/** Where an error reported to `onError` came from. @since 1.0.0 */
export interface ErrorContext {
  readonly where: string;
  readonly channel?: string;
  readonly connection?: string;
}

/** Options of {@link createHub}. @since 1.0.0 */
export interface HubOptions {
  /** Storage and fan-out. Default: a fresh `memoryAdapter()`, closed with the hub. */
  readonly adapter?: Adapter;
  /** Channel rules by pattern, in order. The first matching pattern wins. */
  readonly channels?: Readonly<Record<string, ChannelRule>>;
  /** Limits (see {@link HubLimits}). */
  readonly limits?: Partial<HubLimits>;
  /** Receives failures the hub handled (an adapter outage, a rule predicate that threw). Default: `console.error`. */
  readonly onError?: (error: unknown, context: ErrorContext) => void;
  /** The clock, in epoch milliseconds. Default `Date.now`. */
  readonly now?: () => number;
}

/** Counters of a running hub. @since 1.0.0 */
export interface HubStats {
  readonly connections: number;
  readonly channels: number;
  readonly subscriptions: number;
  readonly published: number;
  readonly delivered: number;
  readonly refused: number;
  readonly slowConsumers: number;
}

/** One connection, as the transport glue drives it. @since 1.0.0 */
export interface HubConnection {
  readonly id: string;
  readonly identity: Identity;
  /** Hands the hub one text frame from the peer. Frames are handled in order. */
  receive(text: string): void;
  /** Tells the hub the transport closed. Safe to call twice. */
  closed(): void;
  /** Resolves once every frame received so far was handled and every request answered. */
  settled(): Promise<void>;
  /** Resolves when the hub finished releasing what the connection held. */
  readonly done: Promise<void>;
}

/** How a connection differs from a long-lived one. @since 1.0.0 */
export interface ConnectOptions {
  /**
   * A connection that exists for one HTTP request: a request is answered for any identity the channel's `subscribe` rule
   * accepts (there is no subscription to look up), and nothing is kept after it closes.
   */
  readonly stateless?: boolean;
  /** Replaces the connection's own publish bucket, so a caller can share one across requests. */
  readonly publishBucket?: TokenBucket;
  /**
   * The id of a long-lived connection of the same user this request acts for (SSE clients post under their stream's id, so
   * `echo:false` and `publisher` work). Honored only when that connection is open on this server and has the same user.
   */
  readonly actingFor?: string;
}

/** Who to remove subscriptions of. At least one field. @since 1.0.0 */
export interface RevokeTarget {
  readonly user?: string;
  readonly connection?: string;
  readonly channel?: string;
}

/** The server object. @since 1.0.0 */
export interface HubCore {
  /** Adds a channel rule after the ones already registered. */
  channel(pattern: string, rule: ChannelRule): HubCore;
  /** Starts handling a connection. Sends `ready`. */
  connect(transport: Transport, identity?: Identity, options?: ConnectOptions): HubConnection;
  /** Publishes from the server (no rule applies). */
  publish(
    channel: string,
    event: string,
    data: unknown,
    options?: { readonly key?: string },
  ): Promise<PublishResult>;
  /** Removes matching subscriptions (a subscribe still awaiting its rule is refused); each subscriber gets `unsubbed why:"revoked"`. Returns how many were removed. */
  revoke(target: RevokeTarget): number;
  /** Counters. */
  stats(): HubStats;
  /** Closes every connection with 1001, stops timers, removes presence, settles requests. */
  close(): Promise<void>;
  /** @internal Changes the state of a presence member from outside its connection (SSE clients). */
  setMemberState(
    identity: Identity,
    channel: string,
    connectionId: string,
    state: Json,
  ): Promise<void>;
  /** @internal The resolved limits. */
  readonly limits: HubLimits;
  /** @internal The adapter. */
  readonly adapter: Adapter;
  /** @internal Reports an error. */
  report(error: unknown, context: ErrorContext): void;
  /** @internal Matches a channel against the rules. */
  match(
    channel: string,
  ): { readonly rule: ChannelRule; readonly params: Record<string, string> } | undefined;
  /** @internal Evaluates a rule permission for an identity. */
  allowed(
    rule: ChannelRule,
    op: RuleContext["op"],
    channel: string,
    params: Record<string, string>,
    identity: Identity,
  ): Promise<boolean>;
}

interface SubState {
  readonly conn: ConnState;
  readonly channel: ChanState;
  readonly rule: ChannelRule;
  readonly params: Record<string, string>;
  readonly echo: boolean;
  readonly watch: boolean;
  joined: boolean;
  version: number;
  /** The state this connection last published to the presence set, to write it back if the entry expired. */
  state: Json;
  lastSeq: number;
  /** The `at` of the last message delivered: a message with an old seq but a newer `at` means the numbering restarted. */
  lastAt: number;
  /** While true, live messages wait in `buffer` and `pump` delivers in order. */
  pumping: boolean;
  gapOk: boolean;
  buffer: StoredMessage[];
  presenceQueue: PresenceEvent[] | undefined;
  /** The join in progress, so that releasing the subscription waits for it and never leaves a member behind. */
  joining: Promise<void> | undefined;
  active: boolean;
}

interface ChanState {
  readonly name: string;
  readonly subs: Set<SubState>;
  readonly bucket: TokenBucket;
  adapterSub: Promise<Unsubscribe> | undefined;
  presenceSub: Promise<Unsubscribe> | undefined;
  closing: boolean;
  /** Cache of the encoded frame of the message being fanned out. */
  frame: { readonly seq: number; readonly text: string } | undefined;
}

interface ConnState {
  readonly id: string;
  readonly identity: Identity;
  readonly transport: Transport;
  readonly subs: Map<string, SubState>;
  readonly queue: string[];
  readonly publishBucket: TokenBucket;
  /** Spent by `sub` and `unsub` frames: subscription churn costs the adapter (and Redis) work like a publish does. */
  readonly controlBucket: TokenBucket;
  readonly invalid: WindowCounter;
  readonly limited: WindowCounter;
  readonly pending: Set<AbortController>;
  readonly stateless: boolean;
  readonly waiters: (() => void)[];
  running: boolean;
  closed: boolean;
  release: (() => void) | undefined;
  readonly done: Promise<void>;
}

const REPLAY_PAGE = 500;
const MAX_PUMP_BUFFER = 10_000;
const ID_BYTES = 16;

/** A random id: 128 bits, base64url. */
function randomId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(ID_BYTES));
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function encodeFrame(frame: Record<string, unknown>): string {
  return JSON.stringify(frame);
}

function messageFrame(channel: string, message: StoredMessage, replayed: boolean): string {
  return (
    `{"t":"msg","c":${JSON.stringify(channel)},"s":${message.seq},"e":${JSON.stringify(message.event)},"d":${message.data}` +
    (message.publisher === undefined ? "" : `,"p":${JSON.stringify(message.publisher)}`) +
    (message.user === undefined ? "" : `,"u":${JSON.stringify(message.user)}`) +
    (replayed ? ',"r":1}' : "}")
  );
}

/**
 * Creates the hub: the server side of `@shadow-js/realtime`. It owns connections and channel rules, enforces limits and talks to
 * one adapter. Wire it to a transport with `hub.websocket()`, `hub.sse()` and `hub.post()` (Dusk routes) or with `hub.connect`.
 *
 * @param options The adapter, the channel rules and the limits.
 * @returns The hub.
 * @throws RangeError for a limit that is not valid; TypeError for an invalid channel pattern.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 *
 * const hub = createHub({
 *   channels: { "chat:{room}": { subscribe: true, publish: ({ identity }) => identity.user !== undefined } },
 * });
 * const sent: string[] = [];
 * const connection = hub.connect({ send: (frame) => (sent.push(frame), true), close: () => {} });
 * connection.receive('{"t":"sub","n":1,"c":"chat:lobby"}');
 * @since 1.0.0
 */
export function createHubCore(options: HubOptions = {}): HubCore {
  const limits = resolveLimits(options.limits);
  const now = options.now ?? Date.now;
  const ownsAdapter = options.adapter === undefined;
  const adapter =
    options.adapter ??
    memoryAdapter({
      now,
      onError: (error, context) =>
        report(error, { where: `memory.${context.where}`, channel: context.channel }),
    });
  const rules: CompiledRule[] = [];
  const conns = new Map<string, ConnState>();
  const chans = new Map<string, ChanState>();
  /** Subscribes whose rules are being awaited: a revoke issued meanwhile marks them, so the grant cannot outlive it. */
  const judging = new Set<{
    readonly conn: ConnState;
    readonly channel: string;
    revoked: boolean;
  }>();
  /** The tail of each channel's presence joins that are queued or running; an entry leaves when its chain is idle. */
  const joinChains = new Map<string, Promise<void>>();
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  let closing = false;
  let beating = false;
  const counters = { published: 0, delivered: 0, refused: 0, slowConsumers: 0 };

  const report = (error: unknown, context: ErrorContext): void => {
    if (options.onError !== undefined) {
      try {
        options.onError(error, context);
      } catch {
        // A failing reporter must not break the hub.
      }
    } else {
      console.error(`[realtime] ${context.where}`, error);
    }
  };

  function match(
    channel: string,
  ): { rule: ChannelRule; params: Record<string, string> } | undefined {
    for (const compiled of rules) {
      const params = matchRule(compiled, channel);
      if (params !== undefined) return { rule: compiled.rule, params };
    }
    return undefined;
  }

  async function allowed(
    rule: ChannelRule,
    op: RuleContext["op"],
    channel: string,
    params: Record<string, string>,
    identity: Identity,
  ): Promise<boolean> {
    const permission = rule[op];
    if (permission === undefined || permission === false) return false;
    if (permission === true) return true;
    try {
      return (await permission({ channel, params, identity, op })) === true;
    } catch (error) {
      report(error, { where: `rule.${op}`, channel });
      throw new RealtimeError(
        "unavailable",
        __DEV__ ? "the channel rule could not be evaluated" : undefined,
      );
    }
  }

  function send(conn: ConnState, text: string): void {
    if (conn.closed) return;
    try {
      const buffered = conn.transport.buffered?.() ?? 0;
      if (buffered > limits.maxBufferedBytes) {
        counters.slowConsumers += 1;
        drop(conn, 1013, __DEV__ ? "slow consumer" : "");
        return;
      }
      if (conn.transport.send(text)) return;
    } catch (error) {
      // A transport that throws is a broken socket: this connection goes, the others are not affected (guarantee G13). The
      // transport is closed too, or its peer would keep a socket the hub no longer answers on until its own heartbeat gave up.
      report(error, { where: "transport.send", connection: conn.id });
      onTransport(conn, "transport.close", () =>
        conn.transport.close(1011, __DEV__ ? "write failed" : ""),
      );
    }
    void closeConnection(conn);
  }

  /** Runs a call on the transport; a transport that is already gone throws, which is only reported. */
  function onTransport(conn: ConnState, where: string, call: () => unknown): void {
    try {
      call();
    } catch (error) {
      report(error, { where, connection: conn.id });
    }
  }

  function sendFrame(conn: ConnState, frame: Record<string, unknown>): void {
    send(conn, encodeFrame(frame));
  }

  function fail(
    conn: ConnState,
    n: number | undefined,
    code: RealtimeError["code"],
    channel?: string,
    message?: string,
  ): void {
    counters.refused += 1;
    sendFrame(conn, {
      t: "err",
      ...(n === undefined ? {} : { n }),
      ...(channel === undefined ? {} : { c: channel }),
      code,
      ...(message === undefined || message === "" ? {} : { msg: message }),
    });
  }

  /** Sends a final frame, closes the transport and releases the connection. */
  function drop(conn: ConnState, code: number, reason: string): void {
    if (conn.closed) return;
    // The connection is released whatever the transport does, so its failures are only reported.
    onTransport(conn, "transport.send", () =>
      conn.transport.send(encodeFrame({ t: "bye", code, why: reason })),
    );
    onTransport(conn, "transport.close", () => conn.transport.close(code, reason));
    void closeConnection(conn);
  }

  // --- channels ------------------------------------------------------------------------------------------------------

  function channelState(name: string): ChanState {
    let chan = chans.get(name);
    if (chan === undefined || chan.closing) {
      chan = {
        name,
        subs: new Set(),
        bucket: new TokenBucket(
          limits.channelPublishesPerSecond,
          limits.channelPublishesPerSecond,
          now(),
        ),
        adapterSub: undefined,
        presenceSub: undefined,
        closing: false,
        frame: undefined,
      };
      chans.set(name, chan);
    }
    return chan;
  }

  /** Opens one of the channel's two adapter feeds once; a failed attempt is forgotten so the next caller retries. */
  async function ensureFeed(
    chan: ChanState,
    key: "adapterSub" | "presenceSub",
    open: () => Promise<Unsubscribe>,
  ): Promise<void> {
    if (chan[key] === undefined) {
      const opened = open();
      chan[key] = opened;
      opened.catch(() => {
        chan[key] = undefined;
      });
    }
    await chan[key];
  }

  function onMessage(chan: ChanState, message: StoredMessage): void {
    for (const sub of chan.subs) {
      if (!sub.active) continue;
      if (sub.pumping) {
        // A consumer so far behind that it cannot catch up is resubscribed from its last seq instead of holding memory.
        if (sub.buffer.length >= MAX_PUMP_BUFFER) removeSub(sub, "gap");
        else sub.buffer.push(message);
        continue;
      }
      if (message.seq === sub.lastSeq + 1) deliver(sub, message, false);
      else if (message.seq > sub.lastSeq + 1) {
        sub.buffer.push(message);
        void pump(sub, undefined);
      } else if (message.at > sub.lastAt) removeSub(sub, "reset");
      // Any other seq at or below the cursor was already delivered: dropped (guarantee G2).
    }
  }

  function deliver(sub: SubState, message: StoredMessage, replayed: boolean): void {
    sub.lastSeq = message.seq;
    sub.lastAt = Math.max(sub.lastAt, message.at);
    if (!sub.echo && message.publisher === sub.conn.id) return;
    counters.delivered += 1;
    if (replayed) {
      send(sub.conn, messageFrame(sub.channel.name, message, true));
      return;
    }
    const cached = sub.channel.frame;
    const text =
      cached !== undefined && cached.seq === message.seq
        ? cached.text
        : messageFrame(sub.channel.name, message, false);
    if (cached === undefined || cached.seq !== message.seq)
      sub.channel.frame = { seq: message.seq, text };
    send(sub.conn, text);
  }

  /**
   * Delivers, in order and without a gap, the messages after `sub.lastSeq` up to `upTo` and whatever is buffered. Fetches the
   * missing ones from history. When history no longer has them: a subscription that reported a gap skips ahead; any other one
   * is removed with `unsubbed why:"gap"`.
   */
  async function pump(sub: SubState, upTo: number | undefined): Promise<void> {
    if (sub.pumping && upTo === undefined) return;
    sub.pumping = true;
    try {
      while (sub.active) {
        sub.buffer = sub.buffer.filter((message) => message.seq > sub.lastSeq);
        const next = sub.buffer[0];
        if (next !== undefined && next.seq === sub.lastSeq + 1) {
          deliver(sub, next, false);
          sub.buffer.shift();
          continue;
        }
        const target = next === undefined ? (upTo ?? 0) : next.seq - 1;
        if (sub.lastSeq >= target) break;
        const page = await adapter.history(
          sub.channel.name,
          sub.lastSeq,
          Math.min(REPLAY_PAGE, target - sub.lastSeq),
        );
        if (!sub.active) return;
        const first = page.messages[0];
        if (first === undefined || first.seq !== sub.lastSeq + 1) {
          if (!sub.gapOk) {
            removeSub(sub, "gap");
            return;
          }
          // The gap was reported at subscribe time: continue from what is retained.
          sub.lastSeq = (first?.seq ?? target + 1) - 1;
          continue;
        }
        for (const message of page.messages) {
          if (message.seq > target) break;
          deliver(sub, message, true);
        }
      }
    } catch (error) {
      report(error, { where: "pump", channel: sub.channel.name, connection: sub.conn.id });
      if (sub.active) removeSub(sub, "unavailable");
    } finally {
      sub.pumping = false;
    }
    // A message that arrived while the last history page was awaited is in the buffer; one more pass delivers it.
    if (sub.active && sub.buffer.length > 0) void pump(sub, undefined);
  }

  function onPresence(chan: ChanState, event: PresenceEvent): void {
    for (const sub of chan.subs) {
      // A change to this connection's own member made elsewhere (an HTTP post to another server) is what a write-back after an
      // expiry has to carry, so the connection's copy follows it.
      if (
        sub.joined &&
        event.op !== "leave" &&
        event.member.id === sub.conn.id &&
        event.member.v > sub.version
      ) {
        sub.version = event.member.v;
        sub.state = event.member.s;
      }
      if (!sub.active || (!sub.joined && !sub.watch)) continue;
      if (sub.presenceQueue !== undefined) sub.presenceQueue.push(event);
      else sendPresence(sub, event);
    }
  }

  function sendPresence(sub: SubState, event: PresenceEvent): void {
    sendFrame(sub.conn, { t: "pres", c: event.channel, op: event.op, m: event.member });
  }

  /** Removes a subscription from its connection and channel; tells the peer when `why` is given. */
  function removeSub(sub: SubState, why?: string): void {
    if (!sub.active) return;
    sub.active = false;
    sub.conn.subs.delete(sub.channel.name);
    sub.channel.subs.delete(sub);
    if (why !== undefined && !sub.conn.closed)
      sendFrame(sub.conn, { t: "unsubbed", c: sub.channel.name, ...(why === "" ? {} : { why }) });
    void releaseSub(sub);
  }

  /** The releases still running, so that `close` finishes them before it closes the adapter. */
  const releasing = new Set<Promise<void>>();

  function releaseSub(sub: SubState): Promise<void> {
    const running = release(sub);
    releasing.add(running);
    void running.finally(() => releasing.delete(running));
    return running;
  }

  async function release(sub: SubState): Promise<void> {
    const chan = sub.channel;
    try {
      // A join that is still being written must finish first, or its member would outlive the connection.
      await sub.joining?.catch(() => undefined);
      if (sub.joined) {
        sub.joined = false;
        await adapter.presenceRemove(chan.name, sub.conn.id);
      }
    } catch (error) {
      report(error, { where: "presenceRemove", channel: chan.name, connection: sub.conn.id });
    }
    if (chan.subs.size > 0 || chan.closing) return;
    chan.closing = true;
    if (chans.get(chan.name) === chan) chans.delete(chan.name);
    const stops = [chan.adapterSub, chan.presenceSub].filter(
      (item): item is Promise<Unsubscribe> => item !== undefined,
    );
    await Promise.all(
      stops.map(async (stop) => {
        try {
          await (await stop)();
        } catch (error) {
          report(error, { where: "unsubscribe", channel: chan.name });
        }
      }),
    );
  }

  // --- frames --------------------------------------------------------------------------------------------------------

  function identityDenied(conn: ConnState): RealtimeError["code"] {
    return conn.identity.user === undefined ? "unauthorized" : "forbidden";
  }

  async function subscribe(
    conn: ConnState,
    frame: Extract<ClientFrame, { t: "sub" }>,
  ): Promise<void> {
    const { c: name, n } = frame;
    if (conn.subs.has(name))
      return fail(conn, n, "conflict", name, __DEV__ ? "already subscribed" : undefined);
    if (conn.subs.size >= limits.maxChannelsPerConnection)
      return fail(
        conn,
        n,
        "limit",
        name,
        __DEV__ ? "too many channels on this connection" : undefined,
      );
    const found = match(name);
    if (found === undefined) return fail(conn, n, identityDenied(conn), name);
    const wantsPresence = frame.hasPresence || frame.watch === true;
    const pending = { conn, channel: name, revoked: false };
    judging.add(pending);
    try {
      if (
        !(await allowed(found.rule, "subscribe", name, found.params, conn.identity)) ||
        (wantsPresence &&
          !(await allowed(found.rule, "presence", name, found.params, conn.identity))) ||
        pending.revoked
      )
        return fail(conn, n, identityDenied(conn), name);
    } finally {
      judging.delete(pending);
    }
    if (
      frame.hasPresence &&
      byteLength(JSON.stringify(frame.presence ?? null)) > limits.maxStateBytes
    )
      return fail(
        conn,
        n,
        "too_large",
        name,
        __DEV__ ? "the presence state is too large" : undefined,
      );
    if (conn.closed) return;
    const chan = channelState(name);
    if (chan.subs.size >= limits.maxSubscribersPerChannel)
      return fail(conn, n, "limit", name, __DEV__ ? "the channel is full" : undefined);
    const sub: SubState = {
      conn,
      channel: chan,
      rule: found.rule,
      params: found.params,
      echo: frame.echo !== false,
      watch: frame.watch === true,
      joined: false,
      version: 0,
      state: null,
      lastSeq: 0,
      lastAt: now(),
      pumping: true,
      gapOk: false,
      buffer: [],
      presenceQueue: wantsPresence ? [] : undefined,
      joining: undefined,
      active: true,
    };
    conn.subs.set(name, sub);
    chan.subs.add(sub);
    try {
      await ensureFeed(chan, "adapterSub", () =>
        adapter.subscribe(chan.name, (message) => onMessage(chan, message)),
      );
      if (wantsPresence) {
        await ensureFeed(chan, "presenceSub", () =>
          adapter.presenceSubscribe(chan.name, (event) => onPresence(chan, event)),
        );
        startHeartbeat();
      }
      const probe = await adapter.history(name, Number.MAX_SAFE_INTEGER, 0);
      if (!sub.active) return;
      const head = probe.head;
      const after = frame.after;
      let gap = false;
      let reset = false;
      if (after === undefined) {
        sub.lastSeq = head;
      } else if (after > head) {
        reset = true;
        sub.lastSeq = head;
      } else {
        sub.lastSeq = after;
        gap = probe.oldest === 0 ? head > after : probe.oldest > after + 1;
        sub.gapOk = gap;
      }
      sendFrame(conn, {
        t: "subbed",
        n,
        c: name,
        head,
        ...(gap ? { gap: true } : {}),
        ...(reset ? { reset: true } : {}),
      });
      if (frame.hasPresence) {
        sub.joining = joinPresence(sub, frame.presence ?? null);
        await sub.joining;
      }
      if (wantsPresence) await syncPresence(sub);
      if (!sub.active) return;
      await pump(sub, head);
      sub.gapOk = false;
    } catch (error) {
      if (sub.active) {
        removeSub(sub);
        report(error, { where: "subscribe", channel: name, connection: conn.id });
        fail(conn, n, error instanceof RealtimeError ? error.code : "unavailable", name);
      }
    }
  }

  /** Joins one at a time per channel: the capacity check reads the set, so two joins in flight would both see room. */
  function joinPresence(sub: SubState, state: Json): Promise<void> {
    const name = sub.channel.name;
    const run = (joinChains.get(name) ?? Promise.resolve()).then(() => joinOne(sub, state));
    const settle = (): void => {
      if (joinChains.get(name) === tail) joinChains.delete(name);
    };
    const tail: Promise<void> = run.then(settle, settle);
    joinChains.set(name, tail);
    return run;
  }

  async function joinOne(sub: SubState, state: Json): Promise<void> {
    if (!sub.active) return;
    const existing = await adapter.presenceList(sub.channel.name);
    if (existing.length >= limits.maxPresenceMembers)
      throw new RealtimeError("limit", __DEV__ ? "the presence set is full" : undefined);
    sub.version = 1;
    sub.state = state;
    sub.joined = true;
    startHeartbeat();
    await adapter.presenceSet(sub.channel.name, memberOf(sub), limits.presenceTtlMs);
  }

  /** The presence member a joined subscription stands for. */
  function memberOf(sub: SubState): Member {
    const { user } = sub.conn.identity;
    return {
      id: sub.conn.id,
      ...(user === undefined ? {} : { u: user }),
      s: sub.state,
      v: sub.version,
    };
  }

  async function syncPresence(sub: SubState): Promise<void> {
    const members = await adapter.presenceList(sub.channel.name);
    if (!sub.active) return;
    sendFrame(sub.conn, { t: "pres", c: sub.channel.name, op: "sync", m: members });
    const queued = sub.presenceQueue ?? [];
    sub.presenceQueue = undefined;
    const known = new Map(members.map((member) => [member.id, member.v]));
    for (const event of queued) {
      // What the snapshot already shows is not announced again.
      const version = known.get(event.member.id);
      if (
        event.op === "leave"
          ? version === undefined
          : version !== undefined && version >= event.member.v
      )
        continue;
      sendPresence(sub, event);
    }
  }

  async function setPresence(
    conn: ConnState,
    frame: Extract<ClientFrame, { t: "pres" }>,
  ): Promise<void> {
    const sub = conn.subs.get(frame.c);
    if (sub === undefined || !sub.joined) return fail(conn, undefined, "not_subscribed", frame.c);
    if (byteLength(JSON.stringify(frame.s)) > limits.maxStateBytes)
      return fail(
        conn,
        undefined,
        "too_large",
        frame.c,
        __DEV__ ? "the presence state is too large" : undefined,
      );
    if (!conn.publishBucket.take(now())) return limit(conn, undefined, frame.c);
    sub.version += 1;
    sub.state = frame.s;
    try {
      await adapter.presenceSet(frame.c, memberOf(sub), limits.presenceTtlMs);
    } catch (error) {
      report(error, { where: "presenceSet", channel: frame.c, connection: conn.id });
      fail(conn, undefined, "unavailable", frame.c);
    }
  }

  function limit(conn: ConnState, n: number | undefined, channel: string): void {
    fail(conn, n, "rate_limited", channel);
    if (conn.limited.hit(now())) drop(conn, 1008, __DEV__ ? "rate limit exceeded" : "");
  }

  async function publishFrame(
    conn: ConnState,
    frame: Extract<ClientFrame, { t: "pub" }>,
  ): Promise<void> {
    const { c: name, n } = frame;
    const found = match(name);
    if (
      found === undefined ||
      !(await allowed(found.rule, "publish", name, found.params, conn.identity))
    )
      return fail(conn, n, identityDenied(conn), name);
    if (frame.d.length > limits.maxMessageBytes)
      return fail(conn, n, "too_large", name, __DEV__ ? "the message is too large" : undefined);
    // A channel with no subscriber on this server has no bucket: only the connection's own rate applies to it.
    if (!conn.publishBucket.take(now()) || chans.get(name)?.bucket.take(now()) === false)
      return limit(conn, n, name);
    try {
      const result = await adapter.publish(
        name,
        {
          event: frame.e,
          data: frame.d,
          publisher: conn.id,
          ...(conn.identity.user === undefined ? {} : { user: conn.identity.user }),
        },
        {
          history: found.rule.history ?? limits.history,
          historyTtlMs: limits.historyTtlMs,
          idempotencyTtlMs: limits.idempotencyTtlMs,
          // An idempotency key is only unique per publisher: the adapters hold one key table per channel, so without a
          // scope a client could mark another user's (or the server's) publish as a duplicate by reusing its key. A
          // signed-in user keeps the scope across connections; anonymous connections share one. Keys never contain
          // "/", so `key/scope` cannot be formed two ways.
          ...(frame.k === undefined
            ? {}
            : {
                key: `${frame.k}/${conn.identity.user === undefined ? "a" : `u:${conn.identity.user}`}`,
              }),
        },
      );
      counters.published += result.duplicate ? 0 : 1;
      if (n !== undefined)
        sendFrame(conn, { t: "ack", n, s: result.seq, ...(result.duplicate ? { dup: true } : {}) });
    } catch (error) {
      report(error, { where: "publish", channel: name, connection: conn.id });
      fail(conn, n, "unavailable", name);
    }
  }

  async function request(
    conn: ConnState,
    frame: Extract<ClientFrame, { t: "req" }>,
  ): Promise<void> {
    const { c: name, n } = frame;
    let target: { rule: ChannelRule; params: Record<string, string> } | undefined =
      conn.subs.get(name);
    if (target === undefined && conn.stateless) {
      const found = match(name);
      if (
        found !== undefined &&
        (await allowed(found.rule, "subscribe", name, found.params, conn.identity))
      )
        target = found;
    }
    if (target === undefined)
      return fail(
        conn,
        n,
        identityDenied(conn) === "unauthorized" && conn.stateless
          ? "unauthorized"
          : "not_subscribed",
        name,
      );
    const sub = target;
    const handler = sub.rule.handlers?.[frame.e];
    if (handler === undefined) return fail(conn, n, "no_handler", name);
    if (conn.pending.size >= limits.maxPendingRequests)
      return fail(conn, n, "limit", name, __DEV__ ? "too many requests in flight" : undefined);
    if (!conn.publishBucket.take(now())) return limit(conn, n, name);
    const controller = new AbortController();
    conn.pending.add(controller);
    const timeout = Math.min(frame.ms ?? limits.requestTimeoutMs, limits.requestTimeoutMs);
    const timer = setTimeout(() => controller.abort(new RealtimeError("timeout")), timeout);
    void (async () => {
      try {
        const run = Promise.resolve(
          handler({
            channel: name,
            params: sub.params,
            identity: conn.identity,
            event: frame.e,
            data: frame.d,
            signal: controller.signal,
          }),
        );
        const aborted = new Promise<never>((_resolve, reject) => {
          controller.signal.addEventListener("abort", () => reject(controller.signal.reason), {
            once: true,
          });
        });
        const reply = (await Promise.race([run, aborted])) ?? null;
        const text = JSON.stringify(reply);
        if (text === undefined || byteLength(text) > limits.maxMessageBytes)
          return fail(conn, n, "too_large", name, __DEV__ ? "the reply is too large" : undefined);
        send(conn, `{"t":"rep","n":${n},"d":${text}}`);
      } catch (error) {
        if (error instanceof RealtimeError)
          fail(
            conn,
            n,
            error.code,
            name,
            error.code === "timeout" || error.code === "closed" ? undefined : error.message,
          );
        else {
          report(error, { where: "request", channel: name, connection: conn.id });
          fail(conn, n, "handler_error", name);
        }
      } finally {
        clearTimeout(timer);
        conn.pending.delete(controller);
        settle(conn);
      }
    })();
  }

  /** Answers a frame that cannot be handled; a peer that keeps sending them is dropped. */
  function refuseFrame(
    conn: ConnState,
    n: number | undefined,
    code: RealtimeError["code"],
    why?: string,
  ): void {
    fail(conn, n, code, undefined, why);
    if (conn.invalid.hit(now())) drop(conn, 1008, __DEV__ ? "too many invalid frames" : "");
  }

  async function handle(conn: ConnState, text: string): Promise<void> {
    const decoded = decodeClientFrame(text, limits.maxMessageBytes);
    if (!decoded.ok) {
      refuseFrame(conn, decoded.n, decoded.code, decoded.why);
      return;
    }
    const frame = decoded.frame;
    const n = "n" in frame ? frame.n : undefined;
    // An `unsub` spends a token but is never refused: the client cannot tell a refused one (the answer names no frame), and would
    // believe it left a channel the hub still holds, so its next `sub` of that channel would be a conflict.
    if (
      (frame.t === "sub" || frame.t === "unsub") &&
      !conn.controlBucket.take(now()) &&
      frame.t === "sub"
    )
      return limit(conn, n, frame.c);
    try {
      switch (frame.t) {
        case "hello":
          return;
        case "ping":
          sendFrame(conn, { t: "pong", ...(frame.n === undefined ? {} : { n: frame.n }) });
          return;
        case "sub":
          return await subscribe(conn, frame);
        case "unsub": {
          const sub = conn.subs.get(frame.c);
          if (sub === undefined) return fail(conn, frame.n, "not_subscribed", frame.c);
          removeSub(sub);
          return;
        }
        case "pub":
          return await publishFrame(conn, frame);
        case "req":
          return await request(conn, frame);
        case "pres":
          return await setPresence(conn, frame);
      }
    } catch (error) {
      const channel = "c" in frame ? frame.c : undefined;
      if (error instanceof RealtimeError) fail(conn, n, error.code, channel);
      else {
        report(error, {
          where: "frame",
          connection: conn.id,
          ...(channel === undefined ? {} : { channel }),
        });
        fail(conn, n, "unavailable", channel);
      }
    }
  }

  async function run(conn: ConnState): Promise<void> {
    if (conn.running) return;
    conn.running = true;
    try {
      for (let text = conn.queue.shift(); text !== undefined; text = conn.queue.shift()) {
        if (conn.closed) break;
        try {
          await handle(conn, text);
        } catch (error) {
          if (error instanceof RealtimeError) fail(conn, undefined, error.code);
          else {
            report(error, { where: "frame", connection: conn.id });
            fail(conn, undefined, "unavailable");
          }
        }
      }
    } finally {
      conn.running = false;
      settle(conn);
    }
  }

  /** Wakes the callers of `settled` when the connection has nothing left to do. */
  function settle(conn: ConnState): void {
    if (conn.running || conn.queue.length > 0 || conn.pending.size > 0) return;
    for (const wake of conn.waiters.splice(0)) wake();
  }

  async function closeConnection(conn: ConnState): Promise<void> {
    if (conn.closed) return conn.done;
    conn.closed = true;
    if (conns.get(conn.id) === conn) conns.delete(conn.id);
    for (const controller of conn.pending) controller.abort(new RealtimeError("closed"));
    conn.pending.clear();
    conn.queue.length = 0;
    for (const wake of conn.waiters.splice(0)) wake();
    const subs = [...conn.subs.values()];
    for (const sub of subs) {
      sub.active = false;
      sub.channel.subs.delete(sub);
    }
    conn.subs.clear();
    await Promise.allSettled(subs.map((sub) => releaseSub(sub)));
    conn.release?.();
  }

  // --- presence heartbeat --------------------------------------------------------------------------------------------

  function startHeartbeat(): void {
    if (heartbeat !== undefined || closing) return;
    const timer = setInterval(() => void beat(), limits.presenceHeartbeatMs);
    // Node's timer can be unref'd so an idle hub does not keep the process alive; other runtimes return a number.
    (timer as unknown as { unref?: () => void }).unref?.();
    heartbeat = timer;
  }

  async function beat(): Promise<void> {
    if (beating) return;
    beating = true;
    try {
      await beatOnce();
    } finally {
      beating = false;
    }
  }

  async function beatOnce(): Promise<void> {
    const refs: { channel: string; id: string }[] = [];
    const watched = new Set<string>();
    for (const conn of conns.values())
      for (const sub of conn.subs.values()) {
        if (sub.joined) refs.push({ channel: sub.channel.name, id: conn.id });
        if (sub.joined || sub.watch) watched.add(sub.channel.name);
      }
    if (refs.length === 0 && watched.size === 0) {
      clearInterval(heartbeat);
      heartbeat = undefined;
      return;
    }
    try {
      const missing = await adapter.presenceTouch(refs, limits.presenceTtlMs);
      for (const ref of missing) {
        const sub = conns.get(ref.id)?.subs.get(ref.channel);
        if (sub === undefined || !sub.joined) continue;
        await adapter.presenceSet(ref.channel, memberOf(sub), limits.presenceTtlMs);
      }
      for (const channel of watched) await adapter.presenceReap(channel);
    } catch (error) {
      report(error, { where: "presenceHeartbeat" });
    }
  }

  // --- the hub -------------------------------------------------------------------------------------------------------

  for (const [pattern, rule] of Object.entries(options.channels ?? {}))
    rules.push(compileRule(pattern, rule));

  const core: HubCore = {
    limits,
    adapter,
    report,
    match,
    allowed,

    channel(pattern, rule) {
      rules.push(compileRule(pattern, rule));
      return core;
    },

    connect(transport, identity = {}, connectOptions = {}) {
      if (closing) throw new RealtimeError("closed", __DEV__ ? "the hub is closed" : undefined);
      let release: () => void = () => undefined;
      const done = new Promise<void>((resolve) => {
        release = resolve;
      });
      const owner =
        connectOptions.actingFor === undefined ? undefined : conns.get(connectOptions.actingFor);
      const acting =
        owner !== undefined && identity.user !== undefined && owner.identity.user === identity.user;
      const conn: ConnState = {
        id: acting ? (connectOptions.actingFor as string) : randomId(),
        identity,
        transport,
        subs: new Map(),
        queue: [],
        publishBucket:
          connectOptions.publishBucket ??
          new TokenBucket(limits.publishesPerSecond, limits.publishBurst, now()),
        // The refill is the publish rate; the burst always covers a connection declaring all its channels at once.
        controlBucket: new TokenBucket(
          limits.publishesPerSecond,
          Math.max(limits.publishBurst, limits.maxChannelsPerConnection),
          now(),
        ),
        stateless: connectOptions.stateless === true,
        waiters: [],
        invalid: new WindowCounter(10, 60_000),
        limited: new WindowCounter(50, 10_000),
        pending: new Set(),
        running: false,
        closed: false,
        release,
        done,
      };
      // An acting connection shares its owner's id: it must not replace the owner in the table.
      if (!acting) conns.set(conn.id, conn);
      sendFrame(conn, {
        t: "ready",
        v: PROTOCOL_VERSION,
        id: conn.id,
        hb: limits.heartbeatMs,
        max: limits.maxMessageBytes,
      });
      return {
        id: conn.id,
        identity,
        done,
        receive(text) {
          if (conn.closed) return;
          // Judged before it takes a place in the queue: a frame over the limit can never be handled, so it must not be able to
          // fill the queue (or be held until its turn) however many arrive.
          if (byteLength(text) > limits.maxMessageBytes) {
            refuseFrame(conn, undefined, "too_large");
            return;
          }
          if (conn.queue.length >= limits.maxQueuedFrames) {
            drop(conn, 1013, __DEV__ ? "too many frames waiting" : "");
            return;
          }
          conn.queue.push(text);
          void run(conn);
        },
        closed() {
          void closeConnection(conn);
        },
        settled() {
          return new Promise<void>((resolve) => {
            conn.waiters.push(resolve);
            settle(conn);
          });
        },
      };
    },

    async publish(channel, event, data, publishOptions = {}) {
      if (!isChannelName(channel))
        throw new RealtimeError(
          "bad_channel",
          __DEV__ ? `"${channel}" is not a valid channel name` : undefined,
        );
      if (!isEventName(event))
        throw new RealtimeError(
          "bad_frame",
          __DEV__ ? `"${event}" is not a valid event name` : undefined,
        );
      if (publishOptions.key !== undefined && !isIdempotencyKey(publishOptions.key))
        throw new RealtimeError(
          "bad_frame",
          __DEV__ ? "the idempotency key is not valid" : undefined,
        );
      let text: string | undefined;
      try {
        text = JSON.stringify(data === undefined ? null : data);
      } catch (error) {
        throw new TypeError(__DEV__ ? "the data is not JSON" : "REALTIME_DATA", {
          cause: error,
        });
      }
      if (text === undefined)
        throw new TypeError(__DEV__ ? "the data is not JSON" : "REALTIME_DATA");
      if (byteLength(text) > limits.maxMessageBytes)
        throw new RealtimeError("too_large", __DEV__ ? "the message is too large" : undefined);
      const found = match(channel);
      const result = await adapter.publish(
        channel,
        { event, data: text },
        {
          history: found?.rule.history ?? limits.history,
          historyTtlMs: limits.historyTtlMs,
          idempotencyTtlMs: limits.idempotencyTtlMs,
          ...(publishOptions.key === undefined ? {} : { key: `${publishOptions.key}/s` }),
        },
      );
      counters.published += result.duplicate ? 0 : 1;
      return result;
    },

    async setMemberState(identity, channel, connectionId, state) {
      if (identity.user === undefined)
        throw new RealtimeError(
          "unauthorized",
          __DEV__ ? "presence changes over HTTP need a signed-in user" : undefined,
        );
      const found = match(channel);
      if (
        found === undefined ||
        !(await allowed(found.rule, "presence", channel, found.params, identity))
      )
        throw new RealtimeError("forbidden");
      if (byteLength(JSON.stringify(state)) > limits.maxStateBytes)
        throw new RealtimeError(
          "too_large",
          __DEV__ ? "the presence state is too large" : undefined,
        );
      const members = await adapter.presenceList(channel);
      const member = members.find((item) => item.id === connectionId);
      if (member === undefined || member.u !== identity.user)
        throw new RealtimeError("forbidden", __DEV__ ? "that connection is not yours" : undefined);
      // The connection may live on this server: its own copy of the member is what the heartbeat writes back after an expiry, so
      // it moves with the stored one (the same for a connection on another server, through the presence event below).
      const local = conns.get(connectionId)?.subs.get(channel);
      const version = Math.max(member.v, local?.version ?? 0) + 1;
      if (local?.joined === true) {
        local.version = version;
        local.state = state;
      }
      await adapter.presenceSet(
        channel,
        { id: member.id, u: member.u, s: state, v: version },
        limits.presenceTtlMs,
      );
    },

    revoke(target) {
      if (
        target.user === undefined &&
        target.connection === undefined &&
        target.channel === undefined
      )
        throw new TypeError(
          __DEV__ ? "revoke needs a user, a connection or a channel." : "REALTIME_REVOKE",
        );
      const hit = (conn: ConnState, channel: string): boolean =>
        (target.connection === undefined || conn.id === target.connection) &&
        (target.user === undefined || conn.identity.user === target.user) &&
        (target.channel === undefined || channel === target.channel);
      let count = 0;
      for (const item of judging) {
        if (item.revoked || !hit(item.conn, item.channel)) continue;
        item.revoked = true;
        count += 1;
      }
      for (const conn of [...conns.values()]) {
        for (const sub of [...conn.subs.values()]) {
          if (!hit(conn, sub.channel.name)) continue;
          removeSub(sub, "revoked");
          count += 1;
        }
      }
      return count;
    },

    stats() {
      let subscriptions = 0;
      for (const conn of conns.values()) subscriptions += conn.subs.size;
      return {
        connections: conns.size,
        channels: chans.size,
        subscriptions,
        ...counters,
      };
    },

    async close() {
      if (closing) return;
      closing = true;
      if (heartbeat !== undefined) clearInterval(heartbeat);
      heartbeat = undefined;
      const all = [...conns.values()];
      for (const conn of all) drop(conn, 1001, __DEV__ ? "going away" : "");
      await Promise.all(all.map((conn) => conn.done));
      await Promise.allSettled([...releasing]);
      if (ownsAdapter) await adapter.close();
    },
  };
  return core;
}

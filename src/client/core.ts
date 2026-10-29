import { type ErrorCode, RealtimeError } from "../errors";
import {
  byteLength,
  isChannelName,
  isEventName,
  isIdempotencyKey,
  type Json,
  type Member,
  type ServerFrame,
  serverFrameOf,
} from "../protocol";
import { applyPresence } from "./presence";
import { type Link, type LinkEvents, openStream } from "./sse";
import type {
  AnyMessage,
  Channel,
  ChannelDef,
  ChannelOptions,
  ChannelSchema,
  ChannelStatus,
  GapInfo,
  MessageMeta,
  PublishAck,
  Realtime,
  RealtimeOptions,
  Status,
} from "./types";
import { backoff, clientId, resolveUrl, sendUrl } from "./util";
import { openSocket } from "./ws";

/** Close codes that mean the client did something the server will not accept again. */
const FATAL = new Set([1002, 1008, 1009]);
/** Refusals that may heal by asking again. */
const TRANSIENT = new Set<ErrorCode>(["unavailable", "rate_limited", "timeout"]);
/** HTTP sl of a stream request that are worth retrying among the 4xx. */
const RETRYABLE_STATUS = new Set([408, 425, 429]);
const MAX_FRAMES_PER_POST = 64;
const MAX_POST_BYTES = 256_000;
const MAX_STREAM_CHANNELS = 64;
const DEFAULT_REQUEST_MS = 10_000;

type Handler = (data: Json, meta: MessageMeta) => void;
type Timer = ReturnType<typeof setTimeout>;

interface Wire {
  /** The number the subscription went out with; the server answers with it. */
  readonly n: number;
  readonly join: boolean;
  readonly watch: boolean;
  /** The cursor the subscription asked to resume from. */
  readonly after: number | undefined;
  /** The presence state version the server hd for this connection. */
  ver: number;
  /** False until the server can accept presence changes for this connection (over SSE, until the member list arrived). */
  ready: boolean;
}

interface Chan {
  readonly name: string;
  readonly options: ChannelOptions;
  readonly ev: Map<string, Set<Handler>>;
  readonly any: Set<(message: AnyMessage<ChannelDef>) => void>;
  readonly gl: Set<(info: GapInfo) => void>;
  readonly sl: Set<(status: ChannelStatus) => void>;
  readonly presence: Set<(members: readonly Member[]) => void>;
  readonly members: Map<string, Member>;
  list?: readonly Member[] | undefined;
  status: ChannelStatus;
  /** Listeners registered; the channel subscribes while this is above zero. */
  refs: number;
  /** Requests in flight; a stateful connection answers a request only on a subscribed channel. */
  hd: number;
  last: number;
  /** True once a `subbed` arrived since the channel last went idle. */
  sy: boolean;
  state: Json | undefined;
  sv: number;
  w?: Wire | undefined;
  retry?: Timer | undefined;
  fails: number;
  closed: boolean;
  hn?: Channel<ChannelDef> | undefined;
}

interface Op {
  readonly kind: "pub" | "req" | "pres";
  readonly chan: Chan;
  readonly event: string;
  /** The JSON text of the data (or of the presence state). */
  readonly data: string;
  readonly key?: string | undefined;
  readonly ack: boolean;
  readonly ms: number;
  readonly order: number;
  n: number;
  timer?: Timer | undefined;
  settle?: ((error: RealtimeError | undefined, value?: unknown) => void) | undefined;
}

/**
 * Creates the realtime client: one transport (WebSocket, falling back to server-sent ev), reconnects with backoff, a
 * bounded offline queue, resume from the last sequence number and typed channels. It connects when the first channel
 * listens or the first message is sent, never earlier.
 *
 * @param options The stream URL and the policy.
 * @returns The client.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 *
 * type Schema = { [name: `chat:${string}`]: { ev: { message: { text: string } } } };
 * const rt = createRealtime<Schema>({ url: "/rt" });
 * const lobby = rt.channel("chat:lobby");
 * const stop = lobby.on("message", (data) => console.log(data.text));
 * await lobby.publish("message", { text: "hello" });
 * stop();
 * rt.close();
 * @since 1.0.0
 */
export function createRealtime<Schema extends ChannelSchema = ChannelSchema>(
  options: RealtimeOptions,
): Realtime<Schema> {
  const reconnect = options.reconnect ?? {};
  const minDelay = reconnect.minDelayMs ?? 250;
  const maxDelay = reconnect.maxDelayMs ?? 30_000;
  const stableMs = reconnect.stableMs ?? 5000;
  const random = reconnect.random ?? Math.random;
  const maxQueued = options.maxQueued ?? 100;
  const queueTtl = options.queueTtlMs ?? 30_000;
  const ackTtl = options.ackTimeoutMs ?? 10_000;
  const auto = (options.transport ?? "auto") === "auto";
  const id = clientId();
  const chans = new Map<string, Chan>();
  const dirty = new Set<Chan>();
  const queue: Op[] = [];
  const inflight = new Map<number, Op>();
  const statusListeners = new Set<(status: Status) => void>();

  let status: Status = "connecting";
  let mode: "ws" | "sse" = options.transport === "sse" ? "sse" : "ws";
  let finished = false;
  let scheduled = false;
  let link: Link | undefined;
  let gen = 0;
  let ready = false;
  let connId: string | undefined;
  let maxBytes = 65_536;
  let streamSig = "";
  let byeCode: number | undefined;
  let wsFails = 0;
  let attempt = 0;
  let counter = 0;
  let keys = 0;
  let orders = 0;
  /** Heartbeat intervals since the last thing was received. */
  let silent = 0;
  let connectTimer: Timer | undefined;
  let reconnectTimer: Timer | undefined;
  let stableTimer: Timer | undefined;
  let beatTimer: ReturnType<typeof setInterval> | undefined;
  let posting = false;
  /** The POST in flight, which closing the client abandons. */
  let batch: AbortController | undefined;
  let postTimer: Timer | undefined;
  let postFails = 0;
  let listening = false;

  const report = (error: unknown): void => {
    try {
      if (options.onError !== undefined) options.onError(error);
      else console.error("[realtime]", error);
    } catch {
      // A failing reporter must not break the client.
    }
  };
  const guard = (run: () => void): void => {
    try {
      run();
    } catch (error) {
      report(error);
    }
  };
  const doFetch: typeof fetch = (input, init) => (options.fetch ?? globalThis.fetch)(input, init);
  const getHeaders = (): HeadersInit | undefined | Promise<HeadersInit | undefined> =>
    typeof options.headers === "function" ? options.headers() : options.headers;

  const emit = <T>(listeners: Iterable<(value: T) => void>, value: T): void => {
    for (const listener of [...listeners]) guard(() => listener(value));
  };
  const setStatus = (next: Status): void => {
    if (next === status) return;
    status = next;
    if (options.onStatus !== undefined) guard(() => options.onStatus?.(next));
    emit(statusListeners, next);
  };
  const setChanStatus = (chan: Chan, next: ChannelStatus): void => {
    if (chan.status === next) return;
    chan.status = next;
    emit(chan.sl, next);
  };

  // --- operations: publishes and requests, queued while there is no connection --------------------------------------

  /** Settles an operation once; an unacknowledged publish that fails is reported instead. */
  const finish = (op: Op, error: RealtimeError | undefined, value?: unknown): void => {
    clearTimeout(op.timer);
    const settle = op.settle;
    op.settle = undefined;
    if (settle !== undefined) settle(error, value);
    else if (error !== undefined && op.kind === "pub") report(error);
  };
  const arm = (op: Op, ms: number): void => {
    clearTimeout(op.timer);
    op.timer = setTimeout(() => {
      const at = queue.indexOf(op);
      if (at >= 0) queue.splice(at, 1);
      if (inflight.get(op.n) === op) inflight.delete(op.n);
      finish(op, new RealtimeError("timeout"));
    }, ms);
  };
  const take = (n: number): Op | undefined => {
    const op = inflight.get(n);
    if (op !== undefined) inflight.delete(n);
    return op;
  };

  const enqueue = (op: Op): RealtimeError | undefined => {
    if (finished) return new RealtimeError("closed");
    const flowing = ready && mode === "ws";
    if (op.kind !== "pres" && !flowing && queue.length >= maxQueued)
      return new RealtimeError("queue_full");
    if (op.kind === "pres") {
      const old = queue.findIndex((item) => item.kind === "pres" && item.chan === op.chan);
      if (old >= 0) queue.splice(old, 1);
    } else arm(op, queueTtl);
    queue.push(op);
    if (ready) flush();
    else schedule();
    return undefined;
  };

  const newOp = (
    kind: Op["kind"],
    chan: Chan,
    event: string,
    data: string,
    extra: Partial<Pick<Op, "key" | "ack" | "ms" | "settle">>,
  ): Op => ({ kind, chan, event, data, ack: true, ms: 0, order: ++orders, n: 0, ...extra });

  /** Whether the server answers an operation (a request, or a publish that asks for an acknowledgment). */
  const answered = (op: Op): boolean => op.kind === "req" || (op.kind === "pub" && op.ack);
  /** How long an operation that went out waits for its answer: a request's own time and a second, or the acknowledgment time. */
  const waitOf = (op: Op): number => (op.kind === "req" ? op.ms + 1000 : ackTtl);

  /** The frame text of an operation, numbered for an answer. Undefined (and the operation failed) when it is too large. */
  const encode = (op: Op): string | undefined => {
    const c = JSON.stringify(op.chan.name);
    const wantsAnswer = answered(op);
    op.n = wantsAnswer ? ++counter : 0;
    const text =
      op.kind === "pres"
        ? `{"t":"pres","c":${c},"s":${op.data}}`
        : op.kind === "req"
          ? `{"t":"req","n":${op.n},"c":${c},"e":"${op.event}","d":${op.data},"ms":${op.ms}}`
          : `{"t":"pub","c":${c},"e":"${op.event}","d":${op.data}${wantsAnswer ? `,"n":${op.n}` : ""},"k":${JSON.stringify(op.key)}}`;
    if (byteLength(text) > maxBytes) {
      finish(op, new RealtimeError("too_large"));
      return undefined;
    }
    return text;
  };

  /** Records that an operation went out: it now waits for an answer, or is done. */
  const sent = (op: Op): void => {
    if (op.kind === "pres") return;
    if (answered(op)) {
      inflight.set(op.n, op);
      arm(op, waitOf(op));
    } else clearTimeout(op.timer);
  };

  /** Puts operations that went out on a connection that died back at the head of the queue, in their original order. */
  const requeue = (ops: Op[]): void => {
    ops.sort((a, b) => a.order - b.order);
    const again: Op[] = [];
    for (const op of ops) {
      if (op.kind === "req") finish(op, new RealtimeError("closed"));
      else {
        if (op.kind === "pub") arm(op, queueTtl);
        again.push(op);
      }
    }
    queue.unshift(...again);
  };

  // --- channels -----------------------------------------------------------------------------------------------------

  const wants = (chan: Chan): boolean =>
    !chan.closed &&
    (chan.refs > 0 ||
      chan.state !== undefined ||
      chan.options.watch === true ||
      (chan.hd > 0 && mode === "ws"));
  const joins = (chan: Chan): boolean => chan.state !== undefined;
  const watches = (chan: Chan): boolean =>
    !joins(chan) && (chan.presence.size > 0 || chan.options.watch === true);

  const touch = (chan: Chan): void => {
    if (chan.status === "idle" && wants(chan)) setChanStatus(chan, "subscribing");
    dirty.add(chan);
    schedule();
  };

  /** The fields a subscription goes out with, over a WebSocket as a frame and over SSE as a query entry. */
  const subscription = (chan: Chan, n: number): Record<string, unknown> => {
    const join = joins(chan);
    const watch = watches(chan);
    const after = chan.sy ? chan.last : chan.options.replay === true ? 0 : undefined;
    const w: Wire = { n, join, watch, after, ver: chan.sv, ready: !join };
    chan.w = w;
    setChanStatus(chan, chan.sy ? "resuming" : "subscribing");
    return {
      c: chan.name,
      ...(after === undefined ? {} : { after }),
      ...(chan.options.echo === false ? { echo: false } : {}),
      ...(join ? { presence: chan.state } : {}),
      ...(watch ? { watch: true } : {}),
    };
  };

  const sendPresence = (chan: Chan): void => {
    const w = chan.w;
    if (w === undefined || chan.state === undefined) return;
    w.ver = chan.sv;
    const op = newOp("pres", chan, "", JSON.stringify(chan.state), {});
    if (mode === "ws") {
      const text = encode(op);
      if (text !== undefined) link?.send(text);
    } else enqueue(op);
  };

  const goIdle = (chan: Chan): void => {
    chan.sy = false;
    chan.last = 0;
    chan.w = undefined;
    clearTimeout(chan.retry);
    chan.retry = undefined;
    chan.fails = 0;
    if (chan.members.size > 0) {
      chan.members.clear();
      chan.list = undefined;
    }
    setChanStatus(chan, chan.closed ? "closed" : "idle");
  };

  /** Brings the w in line with what a channel wants (WebSocket: subscribe, unsubscribe, presence). */
  const unsub = (chan: Chan): void => {
    link?.send(`{"t":"unsub","c":${JSON.stringify(chan.name)}}`);
  };

  const syncChan = (chan: Chan): void => {
    const want = wants(chan);
    const w = chan.w;
    if (mode === "ws" && ready) {
      if (w !== undefined && (!want || w.join !== joins(chan) || w.watch !== watches(chan))) {
        unsub(chan);
        chan.w = undefined;
      }
      if (want && chan.w === undefined && chan.retry === undefined) {
        const n = ++counter;
        link?.send(JSON.stringify({ t: "sub", n, ...subscription(chan, n) }));
      }
    }
    if (!want) {
      if (chan.status !== "idle" && chan.status !== "closed") goIdle(chan);
      return;
    }
    const current = chan.w;
    if (current?.join && current.ready && current.ver !== chan.sv) sendPresence(chan);
  };

  const syncAll = (): void => {
    const batch = [...dirty];
    dirty.clear();
    for (const chan of batch) syncChan(chan);
  };

  const closeChan = (chan: Chan, error?: RealtimeError): void => {
    if (chan.closed) return;
    chan.closed = true;
    if (chans.get(chan.name) === chan) chans.delete(chan.name);
    if (chan.w !== undefined && mode === "ws" && ready) unsub(chan);
    goIdle(chan);
    chan.ev.clear();
    chan.any.clear();
    chan.gl.clear();
    chan.presence.clear();
    chan.sl.clear();
    chan.refs = 0;
    dirty.delete(chan);
    if (error !== undefined) report(error);
    schedule();
  };

  /** Subscribes again after a refusal or a removal the server announced; `now` skips the delay. */
  const resubscribe = (chan: Chan, now: boolean): void => {
    chan.w = undefined;
    setChanStatus(chan, chan.sy ? "resuming" : "subscribing");
    if (now) {
      touch(chan);
      return;
    }
    chan.retry = setTimeout(
      () => {
        chan.retry = undefined;
        touch(chan);
      },
      backoff(chan.fails, minDelay, maxDelay, random),
    );
    chan.fails += 1;
  };

  // --- connection ---------------------------------------------------------------------------------------------------

  /** What the open stream must be reopened for: the channels it carries and how they join. */
  const signature = (): string => {
    let sig = "";
    for (const chan of chans.values())
      if (wants(chan))
        sig += `${chan.name}|${chan.options.echo === false ? 0 : 1}${joins(chan) ? 1 : 0}${watches(chan) ? 1 : 0};`;
    return sig;
  };

  const connect = (): void => {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    if (finished || link !== undefined) return;
    gen += 1;
    const g = gen;
    ready = false;
    silent = 0;
    const ev: LinkEvents = {
      rx: () => {
        if (g === gen) silent = 0;
      },
      frame: (frame) => {
        if (g === gen) guard(() => onFrame(frame, false));
      },
      closed: (info) => {
        if (g === gen) linkClosed(info);
      },
    };
    try {
      if (!listening && typeof addEventListener === "function") {
        listening = true;
        addEventListener("online", onOnline);
        if (typeof document !== "undefined")
          document.addEventListener("visibilitychange", onVisible);
      }
      if (mode === "sse") {
        const url = resolveUrl(options.url, false);
        // The subscriptions of the stream go in the query: the entries of the channels it carries.
        const entries: unknown[] = [];
        for (const chan of [...chans.values()]) {
          if (!wants(chan)) continue;
          if (entries.length >= MAX_STREAM_CHANNELS) {
            closeChan(chan, new RealtimeError("limit"));
            continue;
          }
          entries.push(subscription(chan, entries.length + 1));
        }
        streamSig = signature();
        url.searchParams.set("s", JSON.stringify(entries));
        link = openStream(url.toString(), doFetch, getHeaders, ev);
      } else
        link = openSocket(
          resolveUrl(options.url, true).toString(),
          options.WebSocket ?? globalThis.WebSocket,
          ev,
        );
    } catch (error) {
      gen += 1;
      shutdown(new RealtimeError("closed", __DEV__ ? "cannot open" : undefined, { cause: error }));
      return;
    }
    connectTimer = setTimeout(() => {
      if (g === gen) linkClosed({});
    }, ackTtl);
  };

  const onOnline = (): void => {
    if (reconnectTimer !== undefined) connect();
  };

  /** A tab that was hidden keeps its timers throttled or frozen: when it shows again, a connection waiting out a backoff reconnects now. */
  const onVisible = (): void => {
    if (document.visibilityState === "visible" && reconnectTimer !== undefined) connect();
  };

  /** Releases the current transport and everything tied to it. */
  const drop = (): void => {
    gen += 1;
    const old = link;
    link = undefined;
    ready = false;
    connId = undefined;
    clearTimeout(connectTimer);
    clearTimeout(stableTimer);
    clearInterval(beatTimer);
    beatTimer = undefined;
    old?.close();
  };

  const linkClosed = (info: { readonly code?: number; readonly status?: number }): void => {
    const wasReady = ready;
    const code = byeCode ?? info.code;
    byeCode = undefined;
    drop();
    if (finished) return;
    if (mode === "ws" && inflight.size > 0) {
      const lost = [...inflight.values()];
      inflight.clear();
      requeue(lost);
    }
    for (const chan of chans.values()) {
      chan.w = undefined;
      clearTimeout(chan.retry);
      chan.retry = undefined;
      if (wants(chan)) setChanStatus(chan, chan.sy ? "resuming" : "subscribing");
    }
    const refused =
      info.status !== undefined &&
      info.status >= 400 &&
      info.status < 500 &&
      !RETRYABLE_STATUS.has(info.status);
    if (((code !== undefined && FATAL.has(code)) || refused) && reconnect.retryFatal !== true) {
      const why = __DEV__
        ? code !== undefined
          ? `the server closed the connection with ${code}`
          : `the server answered ${info.status}`
        : undefined;
      shutdown(new RealtimeError("closed", why));
      return;
    }
    if (attempt >= (reconnect.maxReconnectAttempts ?? Infinity)) {
      shutdown(
        __DEV__
          ? new RealtimeError(
              "closed",
              `gave up after ${reconnect.maxReconnectAttempts} reconnect attempts`,
            )
          : new RealtimeError("closed"),
      );
      return;
    }
    let delay = backoff(attempt, minDelay, maxDelay, random);
    attempt += 1;
    if (wasReady) wsFails = 0;
    else if (auto && mode === "ws") {
      wsFails += 1;
      if (wsFails >= 2) {
        mode = "sse";
        delay = 0;
      }
    }
    setStatus("reconnecting");
    reconnectTimer = setTimeout(connect, delay);
  };

  /** Heartbeat: every interval sends a ping; two intervals with nothing received declare the connection dead. */
  const beat = (): void => {
    // The interval that saw the last frame is the first of the three beats counted before the connection is dead.
    silent += 1;
    if (silent > 2) {
      linkClosed({});
      return;
    }
    if (mode === "ws") link?.send('{"t":"ping"}');
  };

  // --- frames from the server ---------------------------------------------------------------------------------------

  const onFrame = (frame: ServerFrame, viaPost: boolean): void => {
    switch (frame.t) {
      case "ready":
        if (ready) return;
        ready = true;
        connId = frame.id;
        maxBytes = frame.max;
        wsFails = 0;
        silent = 0;
        clearTimeout(connectTimer);
        if (frame.hb > 0) beatTimer = setInterval(beat, frame.hb);
        stableTimer = setTimeout(() => {
          attempt = 0;
        }, stableMs);
        setStatus("open");
        if (mode === "ws") for (const chan of chans.values()) dirty.add(chan);
        flush();
        return;
      case "bye":
        byeCode = frame.code;
        return;
      case "subbed": {
        const chan = chans.get(frame.c);
        const w = chan?.w;
        if (chan === undefined || w === undefined || w.n !== frame.n) return;
        if (frame.reset || w.after === undefined) chan.last = frame.head;
        const gap =
          frame.reset === true ||
          (frame.gap === true && w.after !== undefined && !(w.after === 0 && !chan.sy));
        chan.sy = true;
        chan.fails = 0;
        setChanStatus(chan, "live");
        if (gap)
          emit(chan.gl, { after: w.after ?? 0, head: frame.head, reset: frame.reset === true });
        return;
      }
      case "unsubbed": {
        const chan = chans.get(frame.c);
        if (chan === undefined) return;
        if (frame.why === "revoked") {
          // The hub dropped the subscription: closing must not send an unsub it would answer with not_subscribed.
          chan.w = undefined;
          closeChan(chan, new RealtimeError("forbidden", __DEV__ ? "revoked" : undefined));
        } else resubscribe(chan, frame.why === "gap" || frame.why === "reset");
        return;
      }
      case "msg": {
        const chan = chans.get(frame.c);
        if (chan === undefined || frame.s <= chan.last) return;
        chan.last = frame.s;
        const meta: MessageMeta = {
          seq: frame.s,
          replayed: frame.r === 1,
          ...(frame.p === undefined ? {} : { publisher: frame.p }),
          ...(frame.u === undefined ? {} : { user: frame.u }),
        };
        for (const handler of [...(chan.ev.get(frame.e) ?? [])])
          guard(() => handler(frame.d, meta));
        for (const handler of [...chan.any])
          guard(() => handler({ event: frame.e, data: frame.d, meta } as AnyMessage<ChannelDef>));
        return;
      }
      case "ack": {
        const op = take(frame.n);
        if (op !== undefined)
          finish(op, undefined, { seq: frame.s, duplicate: frame.dup === true });
        return;
      }
      case "rep": {
        const op = take(frame.n);
        if (op !== undefined) finish(op, undefined, frame.d);
        return;
      }
      case "err": {
        const error = new RealtimeError(frame.code, frame.msg);
        const chan = frame.c === undefined ? undefined : chans.get(frame.c);
        if (frame.n !== undefined) {
          if (!viaPost && chan?.w?.n === frame.n) {
            if (TRANSIENT.has(frame.code)) {
              resubscribe(chan, false);
              report(error);
            } else {
              chan.w = undefined;
              closeChan(chan, error);
            }
            return;
          }
          // Over SSE a number on the stream names a subscription; only a POST answer names an operation.
          const op = viaPost || mode === "ws" ? take(frame.n) : undefined;
          if (op !== undefined) {
            finish(op, error);
            return;
          }
        }
        report(error);
        return;
      }
      case "pres": {
        const chan = chans.get(frame.c);
        if (chan === undefined) return;
        if (applyPresence(chan.members, frame.op, frame.m)) {
          chan.list = [...chan.members.values()];
          emit(chan.presence, chan.list);
        }
        if (frame.op === "sync" && chan.w !== undefined) {
          chan.w.ready = true;
          if (chan.w.join && chan.w.ver !== chan.sv) sendPresence(chan);
        }
        return;
      }
      case "pong":
        return;
    }
  };

  // --- sending ------------------------------------------------------------------------------------------------------

  const flush = (): void => {
    if (!ready || finished) return;
    syncAll();
    if (mode === "sse") {
      postNext();
      return;
    }
    for (let op = queue[0]; op !== undefined && ready; op = queue[0]) {
      const text = encode(op);
      if (text !== undefined) {
        if (link?.send(text) !== true) return;
        sent(op);
      }
      queue.shift();
    }
  };

  const postNext = (): void => {
    if (posting || postTimer !== undefined || connId === undefined) return;
    const ops: Op[] = [];
    const texts: string[] = [];
    let bytes = 0;
    while (queue.length > 0 && ops.length < MAX_FRAMES_PER_POST && bytes < MAX_POST_BYTES) {
      const op = queue.shift();
      if (op === undefined) break;
      const text = encode(op);
      if (text === undefined) continue;
      ops.push(op);
      texts.push(text);
      bytes += text.length;
    }
    if (ops.length === 0) return;
    posting = true;
    postBatch(ops, `{"cid":${JSON.stringify(connId)},"frames":[${texts.join(",")}]}`).then(
      undefined,
      report,
    );
  };

  const postBatch = async (ops: Op[], body: string): Promise<void> => {
    for (const op of ops) sent(op);
    let again = false;
    // A request that never answers would hold `posting` for as long as the platform waits (minutes) and nothing behind it
    // could be sent: it is abandoned when the longest wait of its operations is over, and its operations are retried.
    const mine = new AbortController();
    batch = mine;
    const patience = setTimeout(() => mine.abort(), Math.max(...ops.map(waitOf)));
    try {
      const headers = new Headers(await getHeaders());
      headers.set("content-type", "application/json");
      const url =
        options.postUrl === undefined
          ? sendUrl(resolveUrl(options.url, false))
          : resolveUrl(options.postUrl, false);
      const response = await doFetch(url, { method: "POST", headers, body, signal: mine.signal });
      if (response.ok) {
        const parsed = (await response.json()) as { frames?: unknown };
        if (Array.isArray(parsed.frames))
          for (const item of parsed.frames as unknown[]) {
            const frame = serverFrameOf(item);
            if (frame !== undefined) guard(() => onFrame(frame, true));
          }
        postFails = 0;
      } else if (response.status >= 500 || RETRYABLE_STATUS.has(response.status)) again = true;
      else {
        const refusal = (await response.json().catch(() => undefined)) as
          | { error?: { code?: ErrorCode; message?: string } }
          | undefined;
        const code: ErrorCode = refusal?.error?.code ?? "unavailable";
        for (const op of ops) {
          if (inflight.get(op.n) === op) inflight.delete(op.n);
          finish(op, new RealtimeError(code, refusal?.error?.message));
        }
      }
    } catch {
      if (finished) return;
      again = true;
    } finally {
      clearTimeout(patience);
    }
    posting = false;
    const unanswered = ops.filter((op) => answered(op) && inflight.get(op.n) === op);
    for (const op of unanswered) inflight.delete(op.n);
    if (again) {
      for (const op of unanswered)
        if (op.kind === "req") finish(op, new RealtimeError("unavailable"));
      requeue(
        ops.filter(
          (op) => op.kind === "pres" || (op.kind === "pub" && (!op.ack || unanswered.includes(op))),
        ),
      );
      postFails += 1;
      postTimer = setTimeout(
        () => {
          postTimer = undefined;
          flush();
        },
        backoff(postFails - 1, minDelay, maxDelay, random),
      );
    } else for (const op of unanswered) finish(op, new RealtimeError("unavailable"));
    flush();
  };

  // --- lifecycle ----------------------------------------------------------------------------------------------------

  const run = (): void => {
    scheduled = false;
    if (finished) return;
    syncAll();
    if (link === undefined) {
      if (reconnectTimer === undefined && (queue.length > 0 || [...chans.values()].some(wants)))
        connect();
      return;
    }
    if (
      mode === "sse" &&
      (signature() !== streamSig ||
        [...chans.values()].some((c) => wants(c) && c.w === undefined && c.retry === undefined))
    ) {
      drop();
      for (const chan of chans.values()) chan.w = undefined;
      connect();
    } else flush();
  };

  function schedule(): void {
    if (scheduled || finished) return;
    scheduled = true;
    queueMicrotask(run);
  }

  function shutdown(error?: RealtimeError): void {
    if (finished) return;
    finished = true;
    clearTimeout(reconnectTimer);
    clearTimeout(postTimer);
    reconnectTimer = undefined;
    postTimer = undefined;
    drop();
    batch?.abort();
    if (listening) {
      if (typeof removeEventListener === "function") removeEventListener("online", onOnline);
      if (typeof document !== "undefined")
        document.removeEventListener("visibilitychange", onVisible);
      listening = false;
    }
    const closed = error ?? new RealtimeError("closed");
    const pending = [...queue.splice(0), ...inflight.values()];
    inflight.clear();
    for (const op of pending) {
      if (op.settle === undefined) clearTimeout(op.timer);
      else finish(op, closed);
    }
    for (const chan of [...chans.values()]) closeChan(chan);
    chans.clear();
    dirty.clear();
    setStatus("closed");
    statusListeners.clear();
    if (error !== undefined) report(error);
  }

  // --- the public handles -------------------------------------------------------------------------------------------

  const makeChannel = (name: string, channelOptions: ChannelOptions): Channel<ChannelDef> => {
    const chan: Chan = {
      name,
      options: channelOptions,
      ev: new Map(),
      any: new Set(),
      gl: new Set(),
      sl: new Set(),
      presence: new Set(),
      members: new Map(),
      status: "idle",
      refs: 0,
      hd: 0,
      last: 0,
      sy: false,
      state: channelOptions.presence,
      sv: 0,
      fails: 0,
      closed: false,
    };
    chans.set(name, chan);
    if (wants(chan)) touch(chan);

    /** Registers a listener; unless `passive`, it keeps the channel subscribed until removed. */
    const listen = <T>(
      set: Set<T>,
      item: T,
      passive?: boolean,
      after?: () => void,
    ): (() => void) => {
      if (chan.closed) return () => undefined;
      // A set holds a function once, like `addEventListener`: only the registration that added it counts a hold and may
      // remove it, so adding the same function twice never leaves a hold that nothing can release.
      let owns = !set.has(item);
      set.add(item);
      if (!passive && owns) {
        chan.refs += 1;
        touch(chan);
      }
      return () => {
        if (!owns) return;
        owns = false;
        // The channel may have been closed (its sets cleared and its holds reset) since this was registered.
        if (!set.delete(item) || passive) return;
        chan.refs -= 1;
        after?.();
        touch(chan);
      };
    };

    /** Validates and queues a publish or a request; the promise settles with its answer. */
    const submit = <T>(
      kind: "pub" | "req",
      event: string,
      data: unknown,
      key: string | undefined,
      ack: boolean,
      ms: number,
    ): Promise<T> => {
      let text = "null";
      let refusal: RealtimeError | undefined;
      if (chan.closed) refusal = new RealtimeError("closed");
      else if (!isEventName(event) || (key !== undefined && !isIdempotencyKey(key)))
        refusal = new RealtimeError("bad_frame");
      else
        try {
          text = JSON.stringify(data ?? null) ?? "null";
        } catch (cause) {
          refusal = new RealtimeError("bad_frame", __DEV__ ? "not JSON" : undefined, { cause });
        }
      if (refusal !== undefined) return Promise.reject(refusal);
      return new Promise<T>((resolve, reject) => {
        const request = kind === "req";
        const op = newOp(kind, chan, event, text, {
          key: request ? undefined : (key ?? `${id}:${++keys}`),
          ack,
          ms,
          settle: ack
            ? (error, value) => {
                if (request) {
                  chan.hd -= 1;
                  touch(chan);
                }
                if (error) reject(error);
                else resolve(value as T);
              }
            : undefined,
        });
        if (request) {
          chan.hd += 1;
          touch(chan);
        }
        const refused = enqueue(op);
        if (refused !== undefined) {
          op.settle = undefined;
          if (request) chan.hd -= 1;
          reject(refused);
        } else if (!ack) resolve({ seq: 0, duplicate: false } as T);
      });
    };

    const hn: Channel<ChannelDef> = {
      name,
      get status() {
        return chan.status;
      },
      get lastSeq() {
        return chan.last;
      },
      on(event, handler) {
        let set = chan.ev.get(event);
        if (set === undefined) {
          set = new Set();
          chan.ev.set(event, set);
        }
        const handlers = set;
        return listen(handlers, handler as Handler, false, () => {
          if (handlers.size === 0 && chan.ev.get(event) === handlers) chan.ev.delete(event);
        });
      },
      onAny: (handler) => listen(chan.any, handler as (message: AnyMessage<ChannelDef>) => void),
      onGap: (handler) => listen(chan.gl, handler),
      onStatus: (handler) => listen(chan.sl, handler, true),
      publish: (event, data, publishOptions = {}) =>
        submit<PublishAck>("pub", event, data, publishOptions.key, publishOptions.ack !== false, 0),
      request: (event, data, requestOptions = {}) =>
        submit("req", event, data, undefined, true, requestOptions.timeoutMs ?? DEFAULT_REQUEST_MS),
      presence: {
        members: () => {
          chan.list ??= [...chan.members.values()];
          return chan.list as never;
        },
        set(state) {
          if (chan.closed) return;
          chan.state = state as Json;
          chan.sv += 1;
          touch(chan);
        },
        leave() {
          if (chan.closed || chan.state === undefined) return;
          chan.state = undefined;
          chan.sv += 1;
          touch(chan);
        },
        on: (callback) => listen(chan.presence, callback as (members: readonly Member[]) => void),
      },
      close: () => closeChan(chan),
    };
    chan.hn = hn;
    return hn;
  };

  return {
    channel(name, channelOptions = {}) {
      if (finished) throw new RealtimeError("closed");
      if (!isChannelName(name)) throw new RealtimeError("bad_channel", name);
      const live = chans.get(name);
      return (live?.hn ?? makeChannel(name, channelOptions)) as never;
    },
    get status() {
      return status;
    },
    get connectionId() {
      return connId;
    },
    onStatus(handler) {
      if (!finished) statusListeners.add(handler);
      return () => {
        statusListeners.delete(handler);
      };
    },
    close: () => shutdown(),
  };
}

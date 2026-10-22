/**
 * The Worker side of a channel's subscribe socket: one socket per channel and adapter, shared by every local listener, kept
 * open with backoff, resumed with `after`, and feeding each listener through its own queue.
 *
 * A Worker may use an I/O object (a socket, a stream, a timer) only inside the request that created it. A listener runs in the
 * request that subscribed, so it is called by a loop that was started there and woken by a plain promise: the socket's
 * handler only moves data into queues and never calls a listener itself.
 * @packageDocumentation
 */
import { RealtimeError } from "../errors";
import type { Member, PresenceEvent, StoredMessage } from "../protocol";
import type { ClientSocket } from "./types";
import { decodeLinkFrame } from "./wire";

/** What a link needs from the adapter that owns it. @internal */
export interface LinkHost {
  readonly reconnectMinMs: number;
  readonly reconnectMaxMs: number;
  /** Ping period; 0 turns pings off. */
  readonly keepAliveMs: number;
  readonly connectTimeoutMs: number;
  /** The most events queued for one listener. */
  readonly maxQueued: number;
  now(): number;
  random(): number;
  /** Opens the subscribe socket (not yet accepted). */
  open(channel: string, after: number | undefined): Promise<ClientSocket>;
  /** Runs `task` once after `ms`; returns the way to cancel. Everything scheduled is canceled when the adapter closes. */
  after(ms: number, task: () => void): () => void;
  /** Runs `task` every `ms`; returns the way to cancel. */
  every(ms: number, task: () => void): () => void;
  report(error: unknown, where: string, channel: string): void;
  /** Called once when the link has nothing left to do. */
  forget(link: ChannelLink): void;
}

interface Deferred {
  readonly promise: Promise<void>;
  resolve(): void;
  reject(error: unknown): void;
}

function deferred(): Deferred {
  let resolve: () => void = () => undefined;
  let reject: (error: unknown) => void = () => undefined;
  const promise = new Promise<void>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  // A waiter may give up first; the rejection must not become an unhandled one.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

/** A queue and the loop that empties it, in the request that created it. */
class Pump<T> {
  readonly #queue: T[] = [];
  readonly #max: number;
  #wake: (() => void) | undefined;
  #stopped = false;

  constructor(max: number) {
    this.#max = max;
  }

  push(item: T): void {
    if (this.#stopped || this.#queue.length >= this.#max) return;
    this.#queue.push(item);
    const wake = this.#wake;
    this.#wake = undefined;
    wake?.();
  }

  stop(): void {
    this.#stopped = true;
    this.#queue.length = 0;
    const wake = this.#wake;
    this.#wake = undefined;
    wake?.();
  }

  async run(
    deliver: (item: T) => void,
    report: (error: unknown) => void,
    every: (() => () => void) | undefined,
  ): Promise<void> {
    const cancel = every?.();
    try {
      while (!this.#stopped) {
        const item = this.#queue.shift();
        if (item === undefined) {
          await new Promise<void>((resolve) => {
            this.#wake = resolve;
          });
          continue;
        }
        try {
          deliver(item);
        } catch (error) {
          report(error);
        }
      }
    } finally {
      cancel?.();
    }
  }
}

interface MessageSub {
  readonly listener: (message: StoredMessage) => void;
  readonly pump: Pump<StoredMessage>;
  /** The highest seq queued for this listener; -1 until the link tells the head. */
  last: number;
}

interface PresenceSub {
  readonly listener: (event: PresenceEvent) => void;
  readonly pump: Pump<PresenceEvent>;
}

/** A way to stop one listener. @internal */
export type LinkSubscription = MessageSub | PresenceSub;

type LinkState = "idle" | "connecting" | "open" | "backoff" | "disposed";

/** @internal */
export class ChannelLink {
  readonly channel: string;
  readonly #host: LinkHost;
  readonly #messages = new Set<MessageSub>();
  readonly #presence = new Set<PresenceSub>();
  #state: LinkState = "idle";
  #socket: ClientSocket | undefined;
  #closeOwner: Deferred | undefined;
  #attempt = 0;
  /** The highest seq received on this link: where a reconnect resumes. */
  #last = 0;
  #known = false;
  #members: Map<string, Member> | undefined;
  #lastReceived = 0;
  #failures = 0;
  #retryAt = 0;
  #connectedAt = 0;
  #ready: Deferred = deferred();
  #cancelRetry: (() => void) | undefined;
  #cancelConnect: (() => void) | undefined;
  #cancelKeep: (() => void) | undefined;

  constructor(channel: string, host: LinkHost) {
    this.channel = channel;
    this.#host = host;
  }

  get state(): LinkState {
    return this.#state;
  }

  get size(): number {
    return this.#messages.size + this.#presence.size;
  }

  // --- listeners -----------------------------------------------------------------------------------------------------

  /** Adds a message listener (call it in the request that will receive messages). */
  addMessages(listener: (message: StoredMessage) => void): LinkSubscription {
    const sub: MessageSub = {
      listener,
      pump: new Pump(this.#host.maxQueued),
      last: this.#state === "open" ? this.#last : -1,
    };
    this.#messages.add(sub);
    void sub.pump.run(
      listener,
      (error) => this.#host.report(error, "listener", this.channel),
      this.#nudger(),
    );
    this.#start();
    return sub;
  }

  /** Adds a presence listener (call it in the request that will receive events). */
  addPresence(listener: (event: PresenceEvent) => void): LinkSubscription {
    const sub: PresenceSub = { listener, pump: new Pump(this.#host.maxQueued) };
    this.#presence.add(sub);
    void sub.pump.run(
      listener,
      (error) => this.#host.report(error, "listener", this.channel),
      this.#nudger(),
    );
    this.#start();
    return sub;
  }

  /** Removes a listener; the last one closes the link. */
  remove(sub: LinkSubscription): void {
    sub.pump.stop();
    this.#messages.delete(sub as MessageSub);
    this.#presence.delete(sub as PresenceSub);
    if (this.size === 0) this.dispose(new RealtimeError("closed", "the subscription ended"));
  }

  /** Resolves when the socket is open and has told the channel's head; rejects after `ms`. */
  whenReady(ms: number): Promise<void> {
    if (this.#state === "open") return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const cancel = this.#host.after(ms, () =>
        reject(new RealtimeError("unavailable", "timed out connecting to the channel object")),
      );
      this.#ready.promise.then(
        () => {
          cancel();
          resolve();
        },
        (error: unknown) => {
          cancel();
          reject(error);
        },
      );
    });
  }

  // --- connecting ----------------------------------------------------------------------------------------------------

  /** The pump's periodic check, if pings are on. */
  #nudger(): (() => () => void) | undefined {
    const every = this.#host.keepAliveMs;
    if (every <= 0) return undefined;
    return () => this.#host.every(every, () => this.#check());
  }

  #start(): void {
    if (this.#state === "idle") this.#connect();
  }

  #connect(): void {
    if (this.#state === "disposed") return;
    this.#cancelRetry?.();
    this.#cancelRetry = undefined;
    this.#cancelConnect?.();
    this.#state = "connecting";
    const attempt = ++this.#attempt;
    this.#connectedAt = this.#host.now();
    this.#cancelConnect = this.#host.after(this.#host.connectTimeoutMs, () => {
      if (attempt === this.#attempt && this.#state === "connecting")
        this.#down(attempt, new RealtimeError("unavailable", "the channel object did not answer"));
    });
    this.#host.open(this.channel, this.#known ? this.#last : undefined).then(
      (socket) => this.#opened(attempt, socket),
      (error: unknown) => {
        if (attempt === this.#attempt) this.#down(attempt, error);
      },
    );
  }

  #opened(attempt: number, socket: ClientSocket): void {
    if (attempt !== this.#attempt || this.#state !== "connecting") {
      this.#closeSocket(socket, undefined);
      return;
    }
    const owner = deferred();
    this.#closeOwner = owner;
    this.#socket = socket;
    // Waits in the request that opened the socket, which is the one place guaranteed to be allowed to close it.
    void owner.promise.then(
      () => this.#tryClose(socket),
      () => undefined,
    );
    socket.addEventListener("message", (event) => {
      if (attempt !== this.#attempt || typeof event.data !== "string") return;
      this.#received(attempt, event.data);
    });
    socket.addEventListener("close", () => {
      if (attempt === this.#attempt)
        this.#down(attempt, new RealtimeError("unavailable", "closed"));
    });
    socket.addEventListener("error", () => {
      if (attempt === this.#attempt) this.#down(attempt, new RealtimeError("unavailable", "error"));
    });
    try {
      socket.accept();
    } catch (error) {
      this.#down(attempt, error);
    }
  }

  #tryClose(socket: ClientSocket): boolean {
    try {
      socket.close(1000, "done");
      return true;
    } catch {
      return false;
    }
  }

  /** Closes a socket from wherever this runs; when the runtime refuses, the request that opened it does it. */
  #closeSocket(socket: ClientSocket | undefined, owner: Deferred | undefined): void {
    if (socket === undefined) return;
    if (!this.#tryClose(socket)) owner?.resolve();
  }

  /** The socket failed or closed: schedule a new attempt, or end the link when nobody listens. */
  #down(attempt: number, error: unknown): void {
    if (attempt !== this.#attempt || this.#state === "disposed") return;
    this.#attempt += 1;
    this.#cancelConnect?.();
    this.#cancelConnect = undefined;
    this.#cancelKeep?.();
    this.#cancelKeep = undefined;
    const socket = this.#socket;
    const owner = this.#closeOwner;
    this.#socket = undefined;
    this.#closeOwner = undefined;
    this.#closeSocket(socket, owner);
    // Waiters keep the promise they hold until the link is open; only an opened link needs a new one.
    if (this.#state === "open") this.#ready = deferred();
    if (this.size === 0) {
      this.dispose(error);
      return;
    }
    this.#state = "backoff";
    this.#failures += 1;
    const ceiling = Math.min(
      this.#host.reconnectMaxMs,
      this.#host.reconnectMinMs * 2 ** Math.min(this.#failures - 1, 20),
    );
    const delay = Math.floor(this.#host.random() * ceiling);
    this.#retryAt = this.#host.now() + delay;
    this.#cancelRetry?.();
    this.#cancelRetry = this.#host.after(delay, () => this.#connect());
  }

  /** Reconnects now, without waiting: the object asked for it (a long replay is continued with a new socket). */
  #resume(attempt: number): void {
    if (attempt !== this.#attempt) return;
    this.#attempt += 1;
    const socket = this.#socket;
    const owner = this.#closeOwner;
    this.#socket = undefined;
    this.#closeOwner = undefined;
    this.#cancelKeep?.();
    this.#cancelKeep = undefined;
    this.#closeSocket(socket, owner);
    this.#state = "idle";
    this.#connect();
  }

  /**
   * A health check run by every listener's loop (each in its own request) and by the ping timer: whichever request is alive
   * can repair a link whose own request is gone, because a timer or a socket handler of a finished request never runs again.
   */
  #check(): void {
    const now = this.#host.now();
    const grace = Math.max(1000, this.#host.keepAliveMs);
    switch (this.#state) {
      case "backoff":
        if (now > this.#retryAt + grace) this.#connect();
        return;
      case "connecting":
        if (now > this.#connectedAt + this.#host.connectTimeoutMs * 2) this.#connect();
        return;
      case "open": {
        const keep = this.#host.keepAliveMs;
        if (keep > 0 && now - this.#lastReceived > keep * 3) {
          this.#down(
            this.#attempt,
            new RealtimeError("unavailable", "the channel object went silent"),
          );
          return;
        }
        try {
          this.#socket?.send("ping");
        } catch {
          // Sending from another request is refused; the owner's own ping covers it.
        }
        return;
      }
      default:
        return;
    }
  }

  // --- frames --------------------------------------------------------------------------------------------------------

  #received(attempt: number, text: string): void {
    this.#lastReceived = this.#host.now();
    if (text === "pong") return;
    const frame = decodeLinkFrame(text);
    if (frame === undefined) return;
    switch (frame.t) {
      case "ready": {
        if (this.#known && frame.head < this.#last) {
          // The channel's numbering restarted (it was forgotten): follow it.
          this.#last = frame.head;
          for (const sub of this.#messages) sub.last = Math.min(sub.last, frame.head);
        }
        if (!this.#known) {
          this.#last = frame.head;
          this.#known = true;
        }
        for (const sub of this.#messages) if (sub.last < 0) sub.last = frame.head;
        this.#state = "open";
        this.#failures = 0;
        this.#cancelConnect?.();
        this.#cancelConnect = undefined;
        this.#lastReceived = this.#host.now();
        if (this.#host.keepAliveMs > 0) {
          this.#cancelKeep?.();
          this.#cancelKeep = this.#host.every(this.#host.keepAliveMs, () => this.#check());
        }
        this.#ready.resolve();
        return;
      }
      case "snap": {
        const next = new Map<string, Member>();
        for (const member of frame.members) next.set(member.id, member);
        const before = this.#members;
        this.#members = next;
        if (before === undefined) return;
        for (const [id, member] of before)
          if (!next.has(id)) this.#fanPresence({ channel: this.channel, op: "leave", member });
        for (const [id, member] of next) {
          const old = before.get(id);
          if (old === undefined) this.#fanPresence({ channel: this.channel, op: "join", member });
          else if (member.v > old.v)
            this.#fanPresence({ channel: this.channel, op: "set", member });
        }
        return;
      }
      case "msg": {
        const message = frame.m;
        if (message.seq > this.#last) this.#last = message.seq;
        for (const sub of this.#messages)
          if (message.seq > sub.last) {
            sub.last = message.seq;
            sub.pump.push(message);
          }
        return;
      }
      case "pres": {
        const members = this.#members ?? new Map<string, Member>();
        this.#members = members;
        const known = members.get(frame.member.id);
        if (frame.op === "leave") {
          if (known === undefined) return;
          members.delete(frame.member.id);
        } else {
          if (known !== undefined && known.v >= frame.member.v && frame.op === "join") return;
          if (known !== undefined && known.v > frame.member.v) return;
          members.set(frame.member.id, frame.member);
        }
        this.#fanPresence({ channel: this.channel, op: frame.op, member: frame.member });
        return;
      }
      case "more":
        this.#resume(attempt);
        return;
      case "bye":
        this.#down(attempt, new RealtimeError("unavailable", frame.why));
        return;
    }
  }

  #fanPresence(event: PresenceEvent): void {
    for (const sub of this.#presence) sub.pump.push(event);
  }

  // --- ending --------------------------------------------------------------------------------------------------------

  /** Ends the link: stops every listener, cancels timers and closes the socket. */
  dispose(reason: unknown): void {
    if (this.#state === "disposed") return;
    this.#state = "disposed";
    this.#attempt += 1;
    this.#cancelRetry?.();
    this.#cancelConnect?.();
    this.#cancelKeep?.();
    this.#cancelRetry = undefined;
    this.#cancelConnect = undefined;
    this.#cancelKeep = undefined;
    const socket = this.#socket;
    const owner = this.#closeOwner;
    this.#socket = undefined;
    this.#closeOwner = undefined;
    this.#closeSocket(socket, owner);
    this.#ready.reject(reason);
    for (const sub of this.#messages) sub.pump.stop();
    for (const sub of this.#presence) sub.pump.stop();
    this.#messages.clear();
    this.#presence.clear();
    this.#host.forget(this);
  }
}

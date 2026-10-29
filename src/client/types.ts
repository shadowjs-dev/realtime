import type { Json, Member } from "../protocol";

/**
 * The shape of one channel in a schema: the events it carries, the presence state its members publish and the requests its
 * server handlers answer.
 * @example
 * import type { ChannelDef } from "@shadow-js/realtime/client";
 *
 * type Lobby = {
 *   events: { message: { text: string } };
 *   presence: { typing: boolean };
 *   requests: { shout: { data: string; reply: string } };
 * };
 * const check: ChannelDef = {} as Lobby;
 * @since 1.0.0
 */
export interface ChannelDef {
  readonly events: Record<string, Json>;
  readonly presence?: Json;
  readonly requests?: Record<string, { readonly data: Json; readonly reply: Json }>;
}

/**
 * A schema maps channel names to their definitions. A template-literal key covers every channel of a family:
 * `{ [name: `chat:${string}`]: { events: { message: { text: string } } } }`.
 * @example
 * import { createRealtime, type ChannelSchema } from "@shadow-js/realtime/client";
 *
 * type Schema = {
 *   [name: `chat:${string}`]: { events: { message: { text: string } } };
 * };
 * const rt = createRealtime<Schema>({ url: "/rt" });
 * rt.channel("chat:lobby").publish("message", { text: "hi" });
 * const loose: ChannelSchema = {};
 * @since 1.0.0
 */
export type ChannelSchema = { [name: string]: ChannelDef };

/**
 * The state of the connection: `connecting` before the first open, `open` while a transport is up, `reconnecting` between
 * attempts, `closed` after `close()` or a fatal close.
 * @since 1.0.0
 */
export type Status = "connecting" | "open" | "reconnecting" | "closed";

/**
 * The state of one channel: `idle` (nothing listens), `subscribing` (first subscription in progress), `live`, `resuming`
 * (subscribed before, subscribing again after a reconnect) or `closed`.
 * @since 1.0.0
 */
export type ChannelStatus = "idle" | "subscribing" | "live" | "resuming" | "closed";

/**
 * What a handler learns about a message besides its data.
 * @since 1.0.0
 */
export interface MessageMeta {
  /** The channel's sequence number of the message. */
  readonly seq: number;
  /** The connection id of the publisher, when a connection published it. */
  readonly publisher?: string;
  /** The user of the publisher, when it has one. */
  readonly user?: string;
  /** True when the message came from the replay after a resume, not live. */
  readonly replayed: boolean;
}

/**
 * What an `onAny` handler receives: one of the channel's events with its data.
 * @since 1.0.0
 */
export type AnyMessage<Def extends ChannelDef> = {
  [E in keyof Def["events"] & string]: {
    readonly event: E;
    readonly data: Def["events"][E];
    readonly meta: MessageMeta;
  };
}[keyof Def["events"] & string];

/**
 * What an `onGap` handler learns: messages after `after` may be missing (`reset` is true when the server's log restarted).
 * @since 1.0.0
 */
export interface GapInfo {
  /** The last sequence number this client had seen. */
  readonly after: number;
  /** The channel's head sequence number at the resubscribe. */
  readonly head: number;
  /** True when the log restarted below `after`. */
  readonly reset: boolean;
}

/**
 * The names of a definition's requests.
 * @since 1.0.0
 */
export type RequestsOf<Def extends ChannelDef> = Def extends {
  readonly requests: infer R extends Record<string, { readonly data: Json; readonly reply: Json }>;
}
  ? R
  : string extends keyof Def["events"]
    ? Record<string, { readonly data: Json; readonly reply: Json }>
    : Record<never, never>;

/**
 * The data type of request `E` of a definition.
 * @since 1.0.0
 */
export type RequestData<Def extends ChannelDef, E extends string> = RequestsOf<Def>[E &
  keyof RequestsOf<Def>] extends { readonly data: infer D }
  ? D
  : never;

/**
 * The reply type of request `E` of a definition.
 * @since 1.0.0
 */
export type RequestReply<Def extends ChannelDef, E extends string> = RequestsOf<Def>[E &
  keyof RequestsOf<Def>] extends { readonly reply: infer R }
  ? R
  : never;

/**
 * The presence state type of a definition (`Json` when it declares none).
 * @since 1.0.0
 */
export type PresenceOf<Def extends ChannelDef> = Def extends {
  readonly presence: infer P extends Json;
}
  ? P
  : Json;

/**
 * Options of one channel.
 * @since 1.0.0
 */
export interface ChannelOptions<State extends Json = Json> {
  /** False to not receive this connection's own publishes (default true). */
  readonly echo?: boolean;
  /** Join the presence set with this state as soon as the channel subscribes. */
  readonly presence?: State;
  /** Receive presence events without joining. */
  readonly watch?: boolean;
  /** Replay the retained history on the first subscribe instead of starting at the head. */
  readonly replay?: boolean;
}

/**
 * Options of one publish.
 * @since 1.0.0
 */
export interface PublishOptions {
  /** Wait for the server's acknowledgment (default true). False resolves as soon as the message is queued or written. */
  readonly ack?: boolean;
  /** An idempotency key (1..64 characters); a resend with the same key is applied once. Default: generated. */
  readonly key?: string;
}

/**
 * What an acknowledged publish resolves with.
 * @since 1.0.0
 */
export interface PublishAck {
  /** The sequence number the message got (0 when `ack` was false). */
  readonly seq: number;
  /** True when the key was already accepted and nothing new was stored. */
  readonly duplicate: boolean;
}

/**
 * The presence of one channel: its members and the state this connection publishes.
 * @since 1.0.0
 */
export interface ChannelPresence<State extends Json> {
  /** The members, in the order they appeared. The same array until something changes. */
  members(): readonly Member<State>[];
  /** Joins the presence set, or replaces this connection's state. */
  set(state: State): void;
  /** Leaves the presence set and keeps the channel subscribed. */
  leave(): void;
  /** Calls `callback` with the member list after every change. Returns the function that stops it. */
  on(callback: (members: readonly Member<State>[]) => void): () => void;
}

/**
 * One channel of a {@link Realtime}.
 * @since 1.0.0
 */
export interface Channel<Def extends ChannelDef = ChannelDef> {
  /** The channel name. */
  readonly name: string;
  /** Where the channel is. */
  readonly status: ChannelStatus;
  /** The last sequence number delivered (0 before the first subscribe). */
  readonly lastSeq: number;
  /** Calls `handler` for each message of `event`. The channel subscribes while something listens. */
  on<E extends keyof Def["events"] & string>(
    event: E,
    handler: (data: Def["events"][E], meta: MessageMeta) => void,
  ): () => void;
  /** Calls `handler` for each message of any event. */
  onAny(handler: (message: AnyMessage<Def>) => void): () => void;
  /** Calls `handler` when messages may be missing after a resubscribe. */
  onGap(handler: (info: GapInfo) => void): () => void;
  /** Calls `handler` when the channel's status changes. */
  onStatus(handler: (status: ChannelStatus) => void): () => void;
  /** Publishes a message. Resolves with the sequence number once the server acknowledged it. */
  publish<E extends keyof Def["events"] & string>(
    event: E,
    data: Def["events"][E],
    options?: PublishOptions,
  ): Promise<PublishAck>;
  /** Sends a request to the server handler of `event` and resolves with its reply. */
  request<E extends keyof RequestsOf<Def> & string>(
    event: E,
    data: RequestData<Def, E>,
    options?: { readonly timeoutMs?: number },
  ): Promise<RequestReply<Def, E>>;
  /** The presence set of the channel. */
  readonly presence: ChannelPresence<PresenceOf<Def>>;
  /** Unsubscribes and releases the channel; the handle is not usable afterward. */
  close(): void;
}

/**
 * Options of {@link createRealtime}.
 * @since 1.0.0
 */
export interface RealtimeOptions {
  /** The stream path or URL, for example `"/rt"`. Relative values resolve against the page. */
  readonly url: string;
  /** `"auto"` (default) tries WebSocket and falls back to server-sent events. */
  readonly transport?: "auto" | "ws" | "sse";
  /** Where server-sent-events clients POST (default: `url` plus `/send`). */
  readonly postUrl?: string;
  /** Reconnection policy. */
  readonly reconnect?: {
    readonly minDelayMs?: number;
    readonly maxDelayMs?: number;
    readonly stableMs?: number;
    /**
     * The most reconnects tried in a row after a connection is lost. The count restarts when a connection stays open
     * `stableMs`. After the last one fails the client is closed for good: `status` is `"closed"`, pending calls reject
     * and `onError` receives a `closed` error. Default: unlimited.
     */
    readonly maxReconnectAttempts?: number;
    /** Retry after a close the server marked as a violation (1002, 1008, 1009). Default false. */
    readonly retryFatal?: boolean;
    /** The random source for jitter, in [0, 1). Default `Math.random`. */
    readonly random?: () => number;
  };
  /** Publishes kept while disconnected (default 100). */
  readonly maxQueued?: number;
  /** Milliseconds a queued publish waits for a connection (default 30,000). */
  readonly queueTtlMs?: number;
  /** Milliseconds to wait for an acknowledgment, and for a connection to become ready (default 10,000). */
  readonly ackTimeoutMs?: number;
  /** Headers for the stream and POST requests of the server-sent-events transport (a browser WebSocket cannot send any). */
  readonly headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
  /** The `fetch` to use. Default: the global one. */
  readonly fetch?: typeof fetch;
  /** The WebSocket constructor to use. Default: the global one. */
  readonly WebSocket?: typeof WebSocket;
  /** Receives failures nobody awaits: a refused subscription, a dropped unacknowledged publish, a handler that threw. */
  readonly onError?: (error: unknown) => void;
  /** Receives each change of the connection status. */
  readonly onStatus?: (status: Status) => void;
}

/**
 * The client: one transport, many channels.
 * @since 1.0.0
 */
export interface Realtime<Schema extends ChannelSchema = ChannelSchema> {
  /** The channel `name`; the same handle again while it is open. Options apply when the channel is first created. */
  channel<Name extends keyof Schema & string>(
    name: Name,
    options?: ChannelOptions<PresenceOf<Schema[Name]>>,
  ): Channel<Schema[Name]>;
  /** The connection status. */
  readonly status: Status;
  /** The id of the current connection, once the server accepted it. */
  readonly connectionId: string | undefined;
  /** Calls `handler` when the connection status changes. */
  onStatus(handler: (status: Status) => void): () => void;
  /** Closes the connection and every channel. Pending publishes reject with `closed`. */
  close(): void;
}

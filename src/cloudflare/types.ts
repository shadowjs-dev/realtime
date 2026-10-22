/**
 * The few shapes of the Cloudflare Workers runtime the Durable Object adapter touches. They are declared here, structurally, so
 * the package has no dependency on `@cloudflare/workers-types`: the real `DurableObjectNamespace`, `DurableObjectState` and
 * `DurableObjectStorage` satisfy them, and so do the in-memory doubles the unit tests use.
 * @packageDocumentation
 */

/** Options of {@link ChannelStorage.list}. @since 1.0.0 */
export interface StorageListOptions {
  /** Only keys with this prefix. */
  readonly prefix?: string;
  /** The first key to return (inclusive). */
  readonly start?: string;
  /** The key to stop at (exclusive). */
  readonly end?: string;
  /** The most entries to return. */
  readonly limit?: number;
  /** Return in descending key order. */
  readonly reverse?: boolean;
}

/** The writes available inside {@link ChannelStorage.transaction}. @since 1.0.0 */
export interface StorageTransaction {
  put(entries: Readonly<Record<string, unknown>>): Promise<void>;
  delete(keys: readonly string[]): Promise<number>;
}

/**
 * The part of a Durable Object's storage the channel object uses. Keys sort as strings, so numbers inside keys are zero padded.
 * @since 1.0.0
 */
export interface ChannelStorage {
  get<T>(key: string): Promise<T | undefined>;
  get<T>(keys: readonly string[]): Promise<Map<string, T>>;
  put(entries: Readonly<Record<string, unknown>>): Promise<void>;
  delete(keys: readonly string[]): Promise<number>;
  list<T>(options?: StorageListOptions): Promise<Map<string, T>>;
  transaction<T>(closure: (transaction: StorageTransaction) => Promise<T>): Promise<T>;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number): Promise<void>;
  deleteAlarm(): Promise<void>;
  deleteAll(): Promise<void>;
}

/** A WebSocket as the channel object and the adapter use it. @since 1.0.0 */
export interface ChannelSocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

/** What the object needs from `DurableObjectState`. @since 1.0.0 */
export interface ChannelObjectState {
  readonly storage: ChannelStorage;
  /** Accepts a server-side socket with the Hibernation API: the runtime may evict the object while the socket stays open. */
  acceptWebSocket(socket: ChannelSocket, tags?: string[]): void;
  /** The sockets accepted with `acceptWebSocket` that are still open. */
  getWebSockets(tag?: string): ChannelSocket[];
  /** Answers a fixed request text without waking the object. */
  setWebSocketAutoResponse?(pair?: object): void;
}

/** A response of a Durable Object stub, with the WebSocket of an accepted upgrade. @since 1.0.0 */
export interface ChannelResponse {
  readonly status: number;
  readonly ok: boolean;
  readonly webSocket?: ClientSocket | null;
  text(): Promise<string>;
}

/** The client end of a WebSocket pair, before `accept()`. @since 1.0.0 */
export interface ClientSocket {
  accept(): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "message", listener: (event: { readonly data: unknown }) => void): void;
  addEventListener(type: "close", listener: (event: { readonly code?: number }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
}

/** What a request to a stub carries. @since 1.0.0 */
export interface ChannelRequestInit {
  readonly method?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly body?: string;
  readonly signal?: AbortSignal;
}

/** A Durable Object stub. @since 1.0.0 */
export interface ChannelStub {
  fetch(url: string, init?: ChannelRequestInit): Promise<ChannelResponse>;
}

/**
 * The namespace binding of the channel object (`env.CHANNELS` for a binding named `CHANNELS`): the real
 * `DurableObjectNamespace` fits.
 * @since 1.0.0
 */
export interface ChannelNamespace<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id, options?: { readonly locationHint?: string }): ChannelStub;
}

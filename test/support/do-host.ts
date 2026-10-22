import { type ObjectPlatform, RealtimeChannelObject } from "../../src/cloudflare/object";
import type {
  ChannelNamespace,
  ChannelObjectState,
  ChannelRequestInit,
  ChannelResponse,
  ChannelSocket,
  ChannelStub,
  ClientSocket,
} from "../../src/cloudflare/types";
import { MemoryStorage } from "./do-storage";

type MessageListener = (event: { readonly data: unknown }) => void;
type CloseListener = (event: { readonly code?: number }) => void;

/** The client end of a fake WebSocket pair: it queues what the server sent until `accept()`, like the runtime does. */
export class FakeClient implements ClientSocket {
  readonly received: string[] = [];
  readonly sent: string[] = [];
  closedWith: { code: number | undefined; reason: string | undefined } | undefined;
  #accepted = false;
  readonly #queue: string[] = [];
  readonly #messageListeners: MessageListener[] = [];
  readonly #closeListeners: CloseListener[] = [];
  readonly #errorListeners: ((event: unknown) => void)[] = [];
  readonly server: FakeServer;
  readonly #host: FakeObjectHost;

  constructor(server: FakeServer, host: FakeObjectHost) {
    this.server = server;
    this.#host = host;
  }

  accept(): void {
    this.#accepted = true;
    for (const text of this.#queue.splice(0)) this.#deliver(text);
  }

  /** @internal The server sent `text`. */
  receive(text: string): void {
    if (!this.#accepted) this.#queue.push(text);
    else this.#deliver(text);
  }

  #deliver(text: string): void {
    this.received.push(text);
    queueMicrotask(() => {
      for (const listener of this.#messageListeners) listener({ data: text });
    });
  }

  send(data: string): void {
    if (this.closedWith !== undefined) throw new Error("the socket is closed");
    this.sent.push(data);
    this.#host.object.webSocketMessage(this.server, data);
  }

  close(code?: number, reason?: string): void {
    if (this.closedWith !== undefined) return;
    this.closedWith = { code, reason };
    this.server.open = false;
    this.#host.object.webSocketClose(this.server, code ?? 1005, reason ?? "");
    this.#host.sockets.delete(this.server);
  }

  /** @internal The server closed. */
  serverClosed(code: number): void {
    if (this.closedWith !== undefined) return;
    this.closedWith = { code, reason: undefined };
    queueMicrotask(() => {
      for (const listener of this.#closeListeners) listener({ code });
    });
  }

  addEventListener(type: "message", listener: MessageListener): void;
  addEventListener(type: "close", listener: CloseListener): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: string, listener: (event: never) => void): void {
    if (type === "message") this.#messageListeners.push(listener as MessageListener);
    else if (type === "close") this.#closeListeners.push(listener as CloseListener);
    else this.#errorListeners.push(listener as (event: unknown) => void);
  }
}

/** The server end: what the object holds. */
export class FakeServer implements ChannelSocket {
  open = true;
  readonly sent: string[] = [];
  closedWith: { code: number | undefined; reason: string | undefined } | undefined;
  client: FakeClient | undefined;
  /** Throw on the next `send` calls. */
  failSends = 0;

  send(data: string): void {
    if (!this.open) throw new Error("the socket is closed");
    if (this.failSends > 0) {
      this.failSends -= 1;
      throw new Error("injected send failure");
    }
    this.sent.push(data);
    this.client?.receive(data);
  }

  close(code?: number, reason?: string): void {
    if (!this.open) return;
    this.open = false;
    this.closedWith = { code, reason };
    this.client?.serverClosed(code ?? 1005);
  }
}

class UpgradeResponse extends Response {
  readonly webSocket: unknown;
  constructor(webSocket: unknown) {
    super(null, { status: 200 });
    this.webSocket = webSocket;
  }
  override get status(): number {
    return 101;
  }
  override get ok(): boolean {
    return false;
  }
}

/**
 * A Durable Object instance on a fake runtime: real storage double, accepted sockets that outlive the object instance, and
 * alarms (fired by hand, or by a real timer with `autoAlarm`). `evict()` replaces the object instance with a new one on the
 * same storage and the same open sockets, which is what the runtime does when it hibernates or restarts an object.
 */
export class FakeObjectHost {
  readonly storage: MemoryStorage = new MemoryStorage();
  readonly sockets: Set<FakeServer> = new Set<FakeServer>();
  object: RealtimeChannelObject;
  readonly #platform: ObjectPlatform;
  #alarmTimer: ReturnType<typeof setTimeout> | undefined;
  /** Instances created, 1 + the evictions. */
  instances = 0;
  #pairs: FakeClient[] = [];

  constructor(options: { readonly autoAlarm?: boolean } = {}) {
    this.#platform = {
      createPair: () => {
        const server = new FakeServer();
        const client = new FakeClient(server, this);
        server.client = client;
        this.#pairs.push(client);
        return { client, server };
      },
      upgrade: (client) => new UpgradeResponse(client),
      autoResponse: () => undefined,
    };
    this.object = this.#create();
    if (options.autoAlarm === true)
      this.storage.onAlarm = (at) => {
        if (this.#alarmTimer !== undefined) clearTimeout(this.#alarmTimer);
        this.#alarmTimer = undefined;
        if (at === null) return;
        this.#alarmTimer = setTimeout(
          () => {
            this.#alarmTimer = undefined;
            void this.fireAlarm();
          },
          Math.max(0, at - Date.now()),
        );
      };
  }

  #create(): RealtimeChannelObject {
    this.instances += 1;
    const state: ChannelObjectState = {
      storage: this.storage,
      acceptWebSocket: (socket) => {
        if (socket instanceof FakeServer) this.sockets.add(socket);
      },
      getWebSockets: () => [...this.sockets].filter((socket) => socket.open),
    };
    return new RealtimeChannelObject(state, {}, this.#platform);
  }

  /** The runtime evicted the object and created a fresh instance for the next event. */
  evict(): void {
    this.object = this.#create();
  }

  /** The runtime delivers the alarm. */
  async fireAlarm(): Promise<void> {
    this.storage.consumeAlarm();
    await this.object.alarm();
  }

  /** Fires the alarm if it is due at the current time. */
  async tick(): Promise<boolean> {
    const at = this.storage.alarmAt;
    if (at === null || at > Date.now()) return false;
    await this.fireAlarm();
    return true;
  }

  /** Server ends of the subscribe sockets, newest last. */
  get clients(): readonly FakeClient[] {
    return this.#pairs;
  }

  dispose(): void {
    if (this.#alarmTimer !== undefined) clearTimeout(this.#alarmTimer);
    this.#alarmTimer = undefined;
  }
}

/** One recorded call of a stub. */
export interface RecordedFetch {
  readonly name: string;
  readonly url: string;
  readonly method: string;
  readonly upgrade: boolean;
}

/** A namespace of fake objects, one host per name, with failure injection. */
export class FakeNamespace implements ChannelNamespace<string> {
  readonly hosts: Map<string, FakeObjectHost> = new Map<string, FakeObjectHost>();
  readonly fetches: RecordedFetch[] = [];
  readonly #autoAlarm: boolean;
  /** Make the next `times` fetches fail with a thrown error (a lost connection). */
  failThrow = 0;
  /** Make the next `times` fetches answer this status with a plain body. */
  failStatus: { times: number; status: number } | undefined;
  /** Delay every answer by this many milliseconds. */
  latencyMs = 0;

  constructor(options: { readonly autoAlarm?: boolean } = {}) {
    this.#autoAlarm = options.autoAlarm === true;
  }

  host(name: string): FakeObjectHost {
    let host = this.hosts.get(name);
    if (host === undefined) {
      host = new FakeObjectHost({ autoAlarm: this.#autoAlarm });
      this.hosts.set(name, host);
    }
    return host;
  }

  idFromName(name: string): string {
    return name;
  }

  get(id: string): ChannelStub {
    return {
      fetch: async (url: string, init?: ChannelRequestInit): Promise<ChannelResponse> => {
        const headers = new Headers(init?.headers);
        this.fetches.push({
          name: id,
          url,
          method: init?.method ?? "GET",
          upgrade: headers.get("upgrade") === "websocket",
        });
        if (this.latencyMs > 0) await new Promise((resolve) => setTimeout(resolve, this.latencyMs));
        if (init?.signal?.aborted === true) throw new Error("aborted");
        if (this.failThrow > 0) {
          this.failThrow -= 1;
          throw new Error("Network connection lost");
        }
        if (this.failStatus !== undefined && this.failStatus.times > 0) {
          this.failStatus.times -= 1;
          return new Response("overloaded", { status: this.failStatus.status });
        }
        const request = new Request(url, {
          method: init?.method ?? "GET",
          headers,
          ...(init?.body === undefined ? {} : { body: init.body }),
        });
        const response = await this.host(id).object.fetch(request);
        const socket: unknown = Reflect.get(response, "webSocket");
        return {
          status: response.status,
          ok: response.ok,
          text: () => response.text(),
          webSocket: socket instanceof FakeClient ? socket : null,
        };
      },
    };
  }

  dispose(): void {
    for (const host of this.hosts.values()) host.dispose();
  }
}

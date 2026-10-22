import { RealtimeError } from "../errors";
import { isChannelName } from "../protocol";
import { ChannelCore } from "./core";
import type { ChannelObjectState, ChannelSocket } from "./types";
import { isOperation, MAX_BODY_BYTES, parseRequest, WIRE_VERSION } from "./wire";

/**
 * The pieces of the platform the object creates: a WebSocket pair, the upgrade response and the auto-response pair. The default
 * reads the Workers globals; the unit tests pass their own.
 * @internal
 */
export interface ObjectPlatform {
  createPair(): { readonly client: unknown; readonly server: ChannelSocket };
  upgrade(client: unknown): Response;
  autoResponse(): object | undefined;
}

function workersPlatform(): ObjectPlatform {
  return {
    createPair() {
      const Pair: unknown = Reflect.get(globalThis, "WebSocketPair");
      if (typeof Pair !== "function")
        throw new RealtimeError("unavailable", "WebSocketPair is missing: this is not a Worker");
      const pair: unknown = Reflect.construct(Pair, []);
      const client: unknown = Reflect.get(Object(pair), "0");
      const server: unknown = Reflect.get(Object(pair), "1");
      if (
        typeof server !== "object" ||
        server === null ||
        typeof Reflect.get(server, "send") !== "function" ||
        typeof Reflect.get(server, "close") !== "function"
      )
        throw new TypeError("WebSocketPair returned an unexpected socket");
      return { client, server: server as ChannelSocket };
    },
    upgrade(client) {
      // `webSocket` is a Workers extension of `ResponseInit`; the standard type does not have it.
      return new Response(null, { status: 101, webSocket: client } as unknown as ResponseInit);
    },
    autoResponse() {
      const Pair: unknown = Reflect.get(globalThis, "WebSocketRequestResponsePair");
      return typeof Pair === "function"
        ? (Reflect.construct(Pair, ["ping", "pong"]) as object)
        : undefined;
    },
  };
}

const JSON_HEADERS = { "content-type": "application/json" } as const;

function reply(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

function refusal(code: string, message: string, status: number): Response {
  return reply({ error: { code, message } }, status);
}

const STATUS: Readonly<Record<string, number>> = {
  bad_frame: 400,
  bad_channel: 400,
  too_large: 413,
  limit: 429,
  conflict: 409,
};

/** A close code `close()` may send: the reserved 1005, 1006 and 1015 never go on the wire. */
function replyCode(code: number): number {
  return code >= 1000 && code < 5000 && code !== 1005 && code !== 1006 && code !== 1015
    ? code
    : 1000;
}

function readAfter(url: URL): number | undefined | "bad" {
  const value = url.searchParams.get("after");
  if (value === null) return undefined;
  if (!/^\d{1,16}$/.test(value)) return "bad";
  return Number(value);
}

/**
 * The Durable Object that owns one channel: the single writer of its sequence numbers, its retained history, its idempotency
 * keys and its presence set, and the fan-out to the Workers that subscribed. Re-export it from your Worker and bind it as a
 * SQLite-backed Durable Object; `cloudflareAdapter` talks to it.
 *
 * It keeps everything in Durable Object storage, so it can be evicted and restart at any time, and it uses the WebSocket
 * Hibernation API, so a channel nobody publishes to costs nothing while its subscribers stay connected. An alarm reaps expired
 * presence members and expired retention.
 *
 * @example
 * // worker.ts
 * export { RealtimeChannelObject } from "@shadow-js/realtime/cloudflare";
 *
 * // wrangler.toml
 * // [[durable_objects.bindings]]
 * // name = "CHANNELS"
 * // class_name = "RealtimeChannelObject"
 * // [[migrations]]
 * // tag = "v1"
 * // new_sqlite_classes = ["RealtimeChannelObject"]
 * @since 1.0.0
 */
export class RealtimeChannelObject {
  readonly #state: ChannelObjectState;
  readonly #core: ChannelCore;
  readonly #platform: ObjectPlatform;

  /**
   * @param state The Durable Object state the runtime passes.
   * @param env The Worker's bindings (unused).
   * @param platform The platform seams (the runtime passes none).
   */
  constructor(state: ChannelObjectState, env?: unknown, platform?: ObjectPlatform) {
    void env;
    this.#state = state;
    this.#platform = platform ?? workersPlatform();
    this.#core = new ChannelCore({ storage: state.storage });
    this.#core.adopt(state.getWebSockets());
    const pair = this.#platform.autoResponse();
    if (pair !== undefined) state.setWebSocketAutoResponse?.(pair);
  }

  /**
   * Handles a request from `cloudflareAdapter`: `POST /v1/<operation>` with a JSON body, or `GET /v1/ws` to subscribe.
   *
   * @param request The request.
   * @returns The response.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === `/v${WIRE_VERSION}/ws`) return this.#subscribe(request, url);
    const operation = /^\/v(\d+)\/([a-z-]+)$/.exec(url.pathname);
    if (operation?.[1] !== String(WIRE_VERSION) || !isOperation(operation[2] ?? ""))
      return refusal("bad_frame", "unknown operation", 404);
    const op = operation[2] ?? "";
    if (!isOperation(op)) return refusal("bad_frame", "unknown operation", 404);
    if (request.method !== "POST") return refusal("bad_frame", "use POST", 405);
    const declared = Number(request.headers.get("content-length") ?? 0);
    if (declared > MAX_BODY_BYTES) return refusal("too_large", "the body is too large", 413);
    const text = await request.text();
    if (text.length > MAX_BODY_BYTES) return refusal("too_large", "the body is too large", 413);
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch {
      return refusal("bad_frame", "the body is not JSON", 400);
    }
    const parsed = parseRequest(op, body);
    if (!parsed.ok) return refusal(parsed.code, parsed.message, STATUS[parsed.code] ?? 400);
    const call = parsed.request;
    try {
      switch (call.op) {
        case "publish":
          return reply(
            await this.#core.publish(call.channel, call.entry, call.options, call.idleMs),
          );
        case "history":
          return reply(await this.#core.history(call.channel, call.after, call.limit));
        case "presence-set":
          await this.#core.presenceSet(call.channel, call.member, call.ttlMs, call.graceMs);
          return reply({});
        case "presence-remove":
          return reply({ removed: await this.#core.presenceRemove(call.channel, call.id) });
        case "presence-list":
          return reply({ members: await this.#core.presenceList(call.channel) });
        case "presence-touch":
          return reply({
            missing: await this.#core.presenceTouch(call.channel, call.ids, call.ttlMs),
          });
        case "presence-reap":
          return reply({ members: await this.#core.presenceReap(call.channel) });
      }
    } catch (error) {
      if (error instanceof RealtimeError)
        return refusal(error.code, error.message, STATUS[error.code] ?? 500);
      console.error("[realtime] channel object", error);
      return refusal("unavailable", "the channel object failed", 500);
    }
  }

  async #subscribe(request: Request, url: URL): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
      return refusal("bad_frame", "expected a WebSocket upgrade", 426);
    const channel = url.searchParams.get("c");
    if (!isChannelName(channel)) return refusal("bad_channel", "bad channel name", 400);
    const after = readAfter(url);
    if (after === "bad") return refusal("bad_frame", "bad after", 400);
    const { client, server } = this.#platform.createPair();
    this.#state.acceptWebSocket(server);
    try {
      await this.#core.attach(server, channel, after);
    } catch (error) {
      try {
        server.close(1011, "attach failed");
      } catch {
        // Already closed.
      }
      if (error instanceof RealtimeError)
        return refusal(error.code, error.message, STATUS[error.code] ?? 500);
      console.error("[realtime] channel object", error);
      return refusal("unavailable", "the channel object failed", 500);
    }
    return this.#platform.upgrade(client);
  }

  /**
   * Runtime hook: a message on a subscribe socket. The only message is the keep-alive `ping`, which the runtime normally answers
   * itself without waking the object.
   *
   * @param socket The socket.
   * @param message The message.
   */
  webSocketMessage(socket: ChannelSocket, message: string | ArrayBuffer): void {
    if (message === "ping") {
      try {
        socket.send("pong");
      } catch {
        this.#core.detach(socket);
      }
    }
  }

  /**
   * Runtime hook: a subscribe socket closed.
   *
   * @param socket The socket.
   * @param code The close code the peer sent.
   * @param reason The reason the peer sent.
   */
  webSocketClose(socket: ChannelSocket, code: number, reason: string): void {
    this.#core.detach(socket);
    try {
      socket.close(replyCode(code), reason.slice(0, 100));
    } catch {
      // Already closed.
    }
  }

  /**
   * Runtime hook: a subscribe socket failed.
   *
   * @param socket The socket.
   */
  webSocketError(socket: ChannelSocket): void {
    this.#core.detach(socket);
    try {
      socket.close(1011, "error");
    } catch {
      // Already closed.
    }
  }

  /**
   * Runtime hook: the alarm. Reaps expired presence members (one `leave` each), expired idempotency keys and expired retained
   * messages, and sets the next alarm.
   */
  async alarm(): Promise<void> {
    await this.#core.alarm();
  }
}

import { RealtimeError } from "../errors";
import { byteLength, type Json } from "../protocol";
import type { HubConnection, HubCore, Transport } from "./hub";
import { TokenBucket } from "./limits";
import type { Identity } from "./rules";

/** What a hub needs from the framework's request event. @since 1.0.0 */
export interface RequestLike {
  readonly request: Request;
}

/** How a connection is identified: an identity, or a response that refuses it. @since 1.0.0 */
export type AuthenticateResult = Identity | undefined | Response;

/** Options shared by the transport routes. @since 1.0.0 */
export interface HubRouteOptions<E extends RequestLike> {
  /**
   * Reads the identity from the request (the session cookie, a bearer token). Return a `Response` to refuse (a 401). Return
   * nothing for an anonymous connection. Default: everyone is anonymous, and channel rules decide.
   */
  readonly authenticate?: (event: E) => AuthenticateResult | Promise<AuthenticateResult>;
  /**
   * Origins allowed to open a stream or post (full origins such as `https://app.example.com`). Default: the request's own
   * origin only. A request with no `Origin` header (same-origin GET, a non-browser client) is allowed.
   */
  readonly allowedOrigins?: readonly string[];
}

/** The WebSocket connection the framework hands to handlers (the part this package uses). @since 1.0.0 */
export interface WebSocketLike {
  readonly data: Record<string, unknown>;
  readonly signal: AbortSignal;
  send(message: string | Uint8Array): boolean;
  close(code?: number, reason?: string): void;
}

/** The handlers of a WebSocket route, shaped like Dusk's `WS` export. @since 1.0.0 */
export interface WebSocketRoute<E extends RequestLike> {
  upgrade(event: E): Promise<Response | undefined>;
  open(ws: WebSocketLike, event: E): Promise<void>;
  message(ws: WebSocketLike, message: string | Uint8Array, event: E): void;
  close(ws: WebSocketLike): void;
  error(ws: WebSocketLike, error: unknown, event: E): void;
}

/** The stream a server-sent events route writes to (the part this package uses). @since 1.0.0 */
export interface SseLike {
  send(message: { readonly event: string; readonly data: string }): Promise<void>;
  close(): void;
  readonly signal: AbortSignal;
  readonly closed: boolean;
}

const KEY = "$realtime";
const MAX_FRAMES_PER_POST = 64;
const MAX_SUBSCRIPTIONS_IN_QUERY = 64;
const MAX_BUCKETS = 10_000;

function refuse(status: number, code: RealtimeError["code"], message: string): Response {
  return new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

/** Whether the request's `Origin` header is acceptable. A missing header is allowed (see `HubRouteOptions.allowedOrigins`). */
function originAllowed(request: Request, allowed: readonly string[] | undefined): boolean {
  const origin = request.headers.get("origin");
  if (origin === null) return true;
  if (allowed !== undefined) return allowed.includes(origin);
  try {
    return origin === new URL(request.url).origin;
  } catch {
    return false;
  }
}

async function identify<E extends RequestLike>(
  options: HubRouteOptions<E>,
  event: E,
): Promise<{ readonly identity: Identity } | { readonly refusal: Response }> {
  const result = await options.authenticate?.(event);
  if (result instanceof Response) return { refusal: result };
  return { identity: result ?? {} };
}

/**
 * The WebSocket route of a hub: `export const WS = hub.websocket({ authenticate })` in a Dusk stream file. The upgrade is decided
 * by `authenticate` (a `Response` refuses it) and Dusk's own checks (origin, caps, size); each text frame goes to the hub.
 * Binary frames close the connection with 1003.
 *
 * @param hub The hub.
 * @param options How to identify the connection.
 * @returns Handlers shaped like Dusk's `WS` export.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 *
 * const hub = createHub({ channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * export const WS = hub.websocket();
 * @since 1.0.0
 */
export function websocketRoute<E extends RequestLike>(
  hub: HubCore,
  options: Pick<HubRouteOptions<E>, "authenticate"> = {},
): WebSocketRoute<E> {
  return {
    async upgrade(event) {
      const result = await identify(options, event);
      return "refusal" in result ? result.refusal : undefined;
    },
    async open(ws, event) {
      const result = await identify(options, event);
      if ("refusal" in result) {
        ws.close(1008, "unauthorized");
        return;
      }
      const transport: Transport = {
        send: (frame) => ws.send(frame),
        close: (code, reason) => ws.close(code, reason),
      };
      ws.data[KEY] = hub.connect(transport, result.identity);
    },
    message(ws, message) {
      const connection = ws.data[KEY] as HubConnection | undefined;
      if (connection === undefined) return;
      if (typeof message !== "string") {
        ws.close(1003, "binary frames are not supported");
        return;
      }
      connection.receive(message);
    },
    close(ws) {
      (ws.data[KEY] as HubConnection | undefined)?.closed();
    },
    error(ws, error) {
      hub.report(error, { where: "websocket" });
      (ws.data[KEY] as HubConnection | undefined)?.closed();
    },
  };
}

/**
 * Refuses a stream that has already started (the status 200 is sent before the handler runs): a `bye` with the policy code the
 * client treats as fatal, so it gives up instead of reconnecting for ever, then the end of the stream.
 */
async function refuseStream(stream: SseLike, why: string): Promise<void> {
  try {
    await stream.send({ event: "rt", data: JSON.stringify({ t: "bye", code: 1008, why }) });
  } catch {
    // A client that already left needs no answer.
  }
  stream.close();
}

/** Reads the subscriptions a stream request declares: `?s=[{"c":"chat:lobby","after":12}]`. */
function declared(url: URL): readonly Record<string, unknown>[] | undefined {
  const text = url.searchParams.get("s");
  if (text === null) return [];
  try {
    const value: unknown = JSON.parse(text);
    if (!Array.isArray(value) || value.length > MAX_SUBSCRIPTIONS_IN_QUERY) return undefined;
    return value.map((item) =>
      item !== null && typeof item === "object" ? (item as Record<string, unknown>) : {},
    );
  } catch {
    return undefined;
  }
}

/**
 * The server-sent events route of a hub, for hosts that cannot hold a WebSocket: `export const SSE = hub.sse()`. The channels
 * are declared in the request (`?s=[{"c":"chat:lobby","after":12,"echo":false,"presence":{"typing":false}}]`); to change them the
 * client opens a new stream. Frames go out as events named `rt`; everything the client sends goes to {@link postRoute}.
 *
 * @param hub The hub.
 * @param options How to identify the connection and which origins may connect.
 * @returns A handler shaped like Dusk's `SSE` export.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 *
 * const hub = createHub({ channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * export const SSE = hub.sse();
 * @since 1.0.0
 */
export function sseRoute<E extends RequestLike>(
  hub: HubCore,
  options: HubRouteOptions<E> = {},
): (event: E, stream: SseLike) => Promise<void> {
  return async (event, stream) => {
    if (!originAllowed(event.request, options.allowedOrigins)) {
      hub.report(new RealtimeError("forbidden", "origin refused"), { where: "sse.origin" });
      await refuseStream(stream, "origin not allowed");
      return;
    }
    const result = await identify(options, event);
    if ("refusal" in result) {
      await refuseStream(stream, "unauthorized");
      return;
    }
    const entries = declared(new URL(event.request.url));
    if (entries === undefined) {
      await refuseStream(stream, "bad subscription list");
      return;
    }
    let pendingBytes = 0;
    const transport: Transport = {
      send(frame) {
        if (stream.closed) return false;
        pendingBytes += frame.length;
        stream.send({ event: "rt", data: frame }).then(
          () => {
            pendingBytes -= frame.length;
          },
          () => {
            pendingBytes = Number.MAX_SAFE_INTEGER;
            connection.closed();
          },
        );
        return true;
      },
      close: () => stream.close(),
      buffered: () => pendingBytes,
    };
    const connection = hub.connect(transport, result.identity);
    entries.forEach((entry, at) => {
      connection.receive(JSON.stringify({ t: "sub", n: at + 1, ...entry }));
    });
    if (!stream.signal.aborted)
      await new Promise<void>((resolve) => {
        stream.signal.addEventListener("abort", () => resolve(), { once: true });
      });
    connection.closed();
    await connection.done;
  };
}

/**
 * The HTTP route that carries what an SSE client sends: `export const POST = hub.post()`. The body is
 * `{"cid": "<connection id>", "frames": [ ... ]}` (at most 64 frames of the types `pub`, `req`, `pres`); the answer is
 * `{"frames": [ ... ]}` with the acks, replies and errors. It keeps nothing between requests, so any server can answer it.
 *
 * The request rate of one caller is limited by `key` (default: the user, else all anonymous callers together) with the hub's
 * publish limits; put Dusk's rate limiting in front of the route for per-address limits.
 *
 * @param hub The hub.
 * @param options Identity, origins and the rate-limit key.
 * @returns A handler for a Dusk API route.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 *
 * const hub = createHub({ channels: { "chat:{room}": { subscribe: true, publish: true } } });
 * export const POST = hub.post();
 * @since 1.0.0
 */
export function postRoute<E extends RequestLike>(
  hub: HubCore,
  options: HubRouteOptions<E> & { readonly key?: (event: E, identity: Identity) => string } = {},
): (event: E) => Promise<Response> {
  const buckets = new Map<string, TokenBucket>();
  const bucketFor = (key: string): TokenBucket => {
    let bucket = buckets.get(key);
    if (bucket === undefined) {
      if (buckets.size >= MAX_BUCKETS) {
        const oldest = buckets.keys().next().value;
        if (oldest !== undefined) buckets.delete(oldest);
      }
      bucket = new TokenBucket(hub.limits.publishesPerSecond, hub.limits.publishBurst, Date.now());
      buckets.set(key, bucket);
    }
    return bucket;
  };
  return async (event) => {
    const { request } = event;
    if (!originAllowed(request, options.allowedOrigins))
      return refuse(403, "forbidden", "origin not allowed");
    const result = await identify(options, event);
    if ("refusal" in result) return result.refusal;
    const text = await request.text();
    if (byteLength(text) > hub.limits.maxMessageBytes * 8)
      return refuse(413, "too_large", "the request is too large");
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch {
      return refuse(400, "bad_frame", "the body is not JSON");
    }
    const frames = (body as { frames?: unknown } | null)?.frames;
    const cid = (body as { cid?: unknown } | null)?.cid;
    if (!Array.isArray(frames) || frames.length === 0 || frames.length > MAX_FRAMES_PER_POST)
      return refuse(400, "bad_frame", `frames must hold 1..${MAX_FRAMES_PER_POST} frames`);
    const key = options.key?.(event, result.identity) ?? result.identity.user ?? "anonymous";
    const answers: string[] = [];
    const transport: Transport = {
      send(frame) {
        if (!frame.startsWith('{"t":"ready"')) answers.push(frame);
        return true;
      },
      close: () => undefined,
    };
    const connection = hub.connect(transport, result.identity, {
      stateless: true,
      publishBucket: bucketFor(key),
      ...(typeof cid === "string" ? { actingFor: cid } : {}),
    });
    try {
      for (const frame of frames) {
        const type = (frame as { t?: unknown } | null)?.t;
        if (type === "pres") {
          const item = frame as { c?: unknown; s?: unknown };
          try {
            if (typeof item.c !== "string" || typeof cid !== "string")
              throw new RealtimeError("bad_frame", "pres needs c and the connection id");
            await hub.setMemberState(result.identity, item.c, cid, (item.s ?? null) as Json);
          } catch (error) {
            const code = error instanceof RealtimeError ? error.code : "unavailable";
            answers.push(JSON.stringify({ t: "err", code }));
          }
          continue;
        }
        if (type !== "pub" && type !== "req") {
          answers.push(
            JSON.stringify({ t: "err", code: "bad_frame", msg: "only pub, req and pres" }),
          );
          continue;
        }
        connection.receive(JSON.stringify(frame));
      }
      await connection.settled();
    } finally {
      connection.closed();
    }
    return new Response(`{"frames":[${answers.join(",")}]}`, {
      status: 200,
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    });
  };
}

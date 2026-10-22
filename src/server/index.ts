import {
  type HubRouteOptions,
  postRoute,
  type RequestLike,
  type SseLike,
  sseRoute,
  type WebSocketRoute,
  websocketRoute,
} from "./http";
import { createHubCore, type HubCore, type HubOptions } from "./hub";
import type { Identity } from "./rules";

export type { Adapter } from "../adapter";
export { memoryAdapter } from "../memory";
export type {
  AuthenticateResult,
  HubRouteOptions,
  RequestLike,
  SseLike,
  WebSocketLike,
  WebSocketRoute,
} from "./http";
export type {
  ConnectOptions,
  ErrorContext,
  HubConnection,
  HubOptions,
  HubStats,
  RevokeTarget,
  Transport,
} from "./hub";
export type { HubLimits } from "./limits";
export { DEFAULT_HUB_LIMITS } from "./limits";
export type {
  ChannelRule,
  Identity,
  Permission,
  RequestContext,
  RequestHandler,
  RuleContext,
} from "./rules";

/**
 * The server object of `@shadow-js/realtime`: connections, channel rules, limits and an adapter, with the routes that
 * connect it to a transport.
 * @since 1.0.0
 */
export interface Hub extends HubCore {
  /** Adds a channel rule after the ones already registered. */
  channel(pattern: string, rule: import("./rules").ChannelRule): Hub;
  /** The WebSocket route: `export const WS = hub.websocket()`. */
  websocket<E extends RequestLike = RequestLike>(
    options?: Pick<HubRouteOptions<E>, "authenticate">,
  ): WebSocketRoute<E>;
  /** The server-sent events route: `export const SSE = hub.sse()`. */
  sse<E extends RequestLike = RequestLike>(
    options?: HubRouteOptions<E>,
  ): (event: E, stream: SseLike) => Promise<void>;
  /** The HTTP route for what SSE clients send: `export const POST = hub.post()`. */
  post<E extends RequestLike = RequestLike>(
    options?: HubRouteOptions<E> & { readonly key?: (event: E, identity: Identity) => string },
  ): (event: E) => Promise<Response>;
}

/**
 * Creates the hub: the server side of `@shadow-js/realtime`. It owns connections and channel rules (default deny), enforces the
 * limits, and talks to one adapter (in memory by default; Redis or Cloudflare when several servers share channels).
 *
 * @param options The adapter, the channel rules and the limits.
 * @returns The hub.
 * @throws RangeError for a limit that is not valid; TypeError for an invalid channel pattern.
 * @example
 * import { createHub } from "@shadow-js/realtime/server";
 *
 * const hub = createHub({
 *   channels: {
 *     "chat:{room}": { subscribe: true, publish: ({ identity }) => identity.user !== undefined },
 *   },
 * });
 * await hub.publish("chat:lobby", "message", { text: "hello" });
 * await hub.close();
 * @since 1.0.0
 */
export function createHub(options: HubOptions = {}): Hub {
  const core = createHubCore(options);
  const hub: Hub = Object.assign(core, {
    channel(pattern: string, rule: import("./rules").ChannelRule): Hub {
      core.channel(pattern, rule);
      return hub;
    },
    websocket: <E extends RequestLike = RequestLike>(
      routeOptions?: Pick<HubRouteOptions<E>, "authenticate">,
    ): WebSocketRoute<E> => websocketRoute<E>(core, routeOptions),
    sse: <E extends RequestLike = RequestLike>(routeOptions?: HubRouteOptions<E>) =>
      sseRoute<E>(core, routeOptions),
    post: <E extends RequestLike = RequestLike>(
      routeOptions?: HubRouteOptions<E> & {
        readonly key?: (event: E, identity: Identity) => string;
      },
    ) => postRoute<E>(core, routeOptions),
  });
  return hub;
}

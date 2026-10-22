import { apiRoutes } from "@shadow-js/dusk/features/api-routes";
import { streams } from "@shadow-js/dusk/features/streams";
import { type ServerHandle, serve } from "@shadow-js/dusk/node";
import { createHandler, type DuskRuntimeManifest } from "@shadow-js/dusk/server";
import type { Hub, SseLike, WebSocketLike } from "../../src/server/index";

/** A real Dusk server on a real port, with the hub mounted as the guide shows and controls to break connections. */
export interface NetServer {
  readonly port: number;
  /** The stream URL clients use. */
  readonly url: string;
  /** Points new and existing routes at another hub (the old hub keeps its own connections until it is closed). */
  setHub(hub: Hub): void;
  /** Closes every live WebSocket from the server side with `code`. */
  killSockets(code?: number): number;
  /** Ends every live SSE stream from the server side. */
  killStreams(): number;
  /** The number of live WebSockets and SSE streams. */
  live(): { readonly sockets: number; readonly streams: number };
  close(): Promise<void>;
}

/** Which transports the Dusk stream route offers. */
export interface BootOptions {
  readonly kinds?: readonly ("WS" | "SSE")[];
  readonly user?: (request: Request) => string | undefined;
}

export async function bootDusk(first: Hub, boot: BootOptions = {}): Promise<NetServer> {
  let current = first;
  const sockets = new Set<WebSocketLike>();
  const sseStreams = new Set<SseLike>();
  const authenticate = {
    authenticate: (event: { request: Request }) => {
      const user =
        boot.user?.(event.request) ??
        event.request.headers.get("x-user") ??
        new URL(event.request.url).searchParams.get("user");
      return user === null || user === undefined ? {} : { user };
    },
  };
  const websocketRoutes = new WeakMap<Hub, ReturnType<Hub["websocket"]>>();
  const sseRoutes = new WeakMap<Hub, ReturnType<Hub["sse"]>>();
  const postRoutes = new WeakMap<Hub, ReturnType<Hub["post"]>>();
  const wsOf = (hub: Hub): ReturnType<Hub["websocket"]> => {
    let route = websocketRoutes.get(hub);
    if (route === undefined) {
      route = hub.websocket(authenticate);
      websocketRoutes.set(hub, route);
    }
    return route;
  };
  const sseOf = (hub: Hub): ReturnType<Hub["sse"]> => {
    let route = sseRoutes.get(hub);
    if (route === undefined) {
      route = hub.sse(authenticate);
      sseRoutes.set(hub, route);
    }
    return route;
  };
  const postOf = (hub: Hub): ReturnType<Hub["post"]> => {
    let route = postRoutes.get(hub);
    if (route === undefined) {
      route = hub.post(authenticate);
      postRoutes.set(hub, route);
    }
    return route;
  };
  const kinds = boot.kinds ?? ["WS", "SSE"];
  const proxy = {
    upgrade: (event: never) => wsOf(current).upgrade(event),
    open: async (socket: WebSocketLike, event: never) => {
      sockets.add(socket);
      await wsOf(current).open(socket, event);
    },
    message: (socket: WebSocketLike, message: string | Uint8Array, event: never) =>
      wsOf(current).message(socket, message, event),
    close: (socket: WebSocketLike) => {
      sockets.delete(socket);
      wsOf(current).close(socket);
    },
    error: (socket: WebSocketLike, error: unknown, event: never) =>
      wsOf(current).error(socket, error, event),
  };
  const sse = async (event: never, stream: SseLike): Promise<void> => {
    sseStreams.add(stream);
    try {
      await sseOf(current)(event, stream);
    } finally {
      sseStreams.delete(stream);
    }
  };
  const handler = createHandler({
    features: [streams(), apiRoutes()],
    manifest: {
      routes: [],
      apiRoutes: [
        {
          id: "rt",
          path: "/rt/send",
          load: async () => ({ POST: ((event: never) => postOf(current)(event)) as never }),
        },
      ],
      serverFns: {},
      assets: { entry: "/app.js", css: [], preloads: {}, base: "" },
      App: (props: { children?: unknown }) => props.children,
      streams: [
        {
          id: "/rt",
          path: "/rt",
          kinds: [...kinds],
          load: async () => ({
            ...(kinds.includes("WS") ? { WS: proxy as never } : {}),
            ...(kinds.includes("SSE") ? { SSE: sse as never } : {}),
          }),
        },
      ] satisfies NonNullable<DuskRuntimeManifest["streams"]>,
    },
    dev: true,
    // Node's WebSocket and fetch send no Origin header, as a non-browser client; a browser always sends one.
    security: { csrf: { allowMissingOrigin: true } },
    onError: () => undefined,
    onWarning: () => undefined,
  });
  const handle: ServerHandle = await serve(handler, {
    port: 0,
    host: "127.0.0.1",
    compress: false,
    shutdownSignals: false,
    exitOnSignal: false,
    onError: () => undefined,
  });
  return {
    port: handle.port,
    url: `http://127.0.0.1:${handle.port}/rt`,
    setHub(hub) {
      current = hub;
    },
    killSockets(code = 1011) {
      const all = [...sockets];
      for (const socket of all) socket.close(code, "killed");
      return all.length;
    },
    killStreams() {
      const all = [...sseStreams];
      for (const stream of all) stream.close();
      return all.length;
    },
    live: () => ({ sockets: sockets.size, streams: sseStreams.size }),
    close: () => handle.close(),
  };
}

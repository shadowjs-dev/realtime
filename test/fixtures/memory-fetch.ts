import type { Hub } from "../../src/server/index";
import type { Identity } from "../../src/server/rules";

/** What a test controls about the SSE transport of one hub. */
export interface FetchFactory {
  /** A drop-in for `fetch` that serves the hub's SSE and POST routes in the same process. */
  readonly fetch: typeof fetch;
  /** The URLs of every stream request, oldest first. */
  readonly streams: URL[];
  /** The bodies (parsed) of every POST attempt, oldest first, including the ones made to fail. */
  readonly posts: { readonly url: URL; readonly body: { cid?: string; frames: Record<string, unknown>[] } }[];
  /** Ends every open stream as a network failure would. */
  dropStreams(): void;
  /** Makes the next `count` POSTs fail as a network error. */
  failPosts(count: number): void;
  /** Makes the next `count` POSTs answer with this HTTP status (and error code). */
  answerPosts(count: number, status: number, code?: string): void;
  /** The headers of every request, oldest first. */
  readonly headers: Headers[];
  /** Makes the next stream requests answer with this HTTP status (0 stops). */
  refuseStreams(status: number): void;
  /** The number of streams that are open. */
  open(): number;
}

/**
 * A `fetch` whose server is a hub in the same process: `GET` on the stream path opens a real SSE stream through `hub.sse()`,
 * `POST` on `<path>/send` goes through `hub.post()`. Nothing about the hub is faked.
 */
export function fetchFactory(hub: () => Hub, identity: Identity = {}): FetchFactory {
  const streams: URL[] = [];
  const posts: FetchFactory["posts"] = [];
  const closers = new Set<() => void>();
  let failing = 0;
  let answering = { count: 0, status: 200, code: "unavailable" };
  const headers: Headers[] = [];
  let refused = 0;
  const encoder = new TextEncoder();

  const routes = new WeakMap<Hub, { sse: ReturnType<Hub["sse"]>; post: ReturnType<Hub["post"]> }>();
  const routesOf = (): { sse: ReturnType<Hub["sse"]>; post: ReturnType<Hub["post"]> } => {
    let found = routes.get(hub());
    if (found === undefined) {
      found = {
        sse: hub().sse({ authenticate: () => identity }),
        post: hub().post({ authenticate: () => identity }),
      };
      routes.set(hub(), found);
    }
    return found;
  };
  const route = (): ReturnType<Hub["sse"]> => routesOf().sse;
  const post = (): ReturnType<Hub["post"]> => routesOf().post;

  const serve: typeof fetch = async (input, init) => {
    const request = new Request(input as string | URL, init);
    const url = new URL(request.url);
    headers.push(request.headers);
    if (request.method === "POST") {
      posts.push({ url, body: JSON.parse(await request.clone().text()) });
      if (failing > 0) {
        failing -= 1;
        throw new TypeError("fetch failed");
      }
      if (answering.count > 0) {
        answering = { ...answering, count: answering.count - 1 };
        return new Response(JSON.stringify({ error: { code: answering.code } }), {
          status: answering.status,
          headers: { "content-type": "application/json" },
        });
      }
      return post()({ request });
    }
    if (refused > 0) return new Response("no", { status: refused });
    streams.push(url);
    const abort = new AbortController();
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
    let closed = false;
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
      },
      cancel() {
        abort.abort();
      },
    });
    const end = (): void => {
      if (closed) return;
      closed = true;
      closers.delete(end);
      abort.abort();
      try {
        controller?.close();
      } catch {
        // already closed
      }
    };
    closers.add(end);
    request.signal.addEventListener("abort", end, { once: true });
    const stream = {
      signal: abort.signal,
      get closed() {
        return closed;
      },
      send: async (message: { event: string; data: string }) => {
        if (closed) throw new Error("closed");
        controller?.enqueue(encoder.encode(`event: ${message.event}\ndata: ${message.data}\n\n`));
      },
      close: end,
    };
    void route()({ request }, stream).then(end, end);
    return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
  };

  return {
    fetch: serve,
    streams,
    posts,
    dropStreams() {
      for (const close of [...closers]) close();
    },
    failPosts(count) {
      failing = count;
    },
    answerPosts(count, status, code = "unavailable") {
      answering = { count, status, code };
    },
    headers,
    refuseStreams(status) {
      refused = status;
    },
    open: () => closers.size,
  };
}

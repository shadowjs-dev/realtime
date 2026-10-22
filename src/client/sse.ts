import { decodeServerFrame, type ServerFrame } from "../protocol";

/** What a transport reports to the client. @internal */
export interface LinkEvents {
  /** Any bytes arrived (a comment counts: it proves the stream is alive). */
  rx(): void;
  frame(frame: ServerFrame): void;
  /** The transport ended. `code` is a WebSocket close code, `status` an HTTP status. */
  closed(info: { readonly code?: number; readonly status?: number }): void;
}

/** One open transport. @internal */
export interface Link {
  send(text: string): boolean;
  close(): void;
}

/** Splits an event stream into `rt` events; keeps a partial line (and a trailing CR) for the next chunk. @internal */
export function sseParser(onData: (data: string) => void): (chunk: string) => void {
  let buffer = "";
  let name = "";
  let data: string[] = [];
  return (chunk) => {
    buffer += chunk;
    let start = 0;
    for (let at = 0; at < buffer.length; at += 1) {
      const char = buffer[at];
      if (char !== "\n" && char !== "\r") continue;
      if (char === "\r" && at === buffer.length - 1) break;
      const line = buffer.slice(start, at);
      if (char === "\r" && buffer[at + 1] === "\n") at += 1;
      start = at + 1;
      if (line === "") {
        if (name === "rt" && data.length > 0) onData(data.join("\n"));
        name = "";
        data = [];
        continue;
      }
      if (line[0] === ":") continue;
      const colon = line.indexOf(":");
      const key = colon < 0 ? line : line.slice(0, colon);
      let value = colon < 0 ? "" : line.slice(colon + 1);
      if (value[0] === " ") value = value.slice(1);
      if (key === "event") name = value;
      else if (key === "data") data.push(value);
    }
    buffer = buffer.slice(start);
  };
}

/** Reads a server-sent events stream with `fetch`, so headers work. Frames are events named `rt`. @internal */
export function openStream(
  url: string,
  send: typeof fetch,
  getHeaders: () => HeadersInit | undefined | Promise<HeadersInit | undefined>,
  events: LinkEvents,
): Link {
  const control = new AbortController();
  const run = async (): Promise<void> => {
    const headers = new Headers(await getHeaders());
    headers.set("accept", "text/event-stream");
    const response = await send(url, { headers, signal: control.signal, cache: "no-store" });
    if (!response.ok || response.body === null) {
      void response.body?.cancel().catch(() => undefined);
      return events.closed({ status: response.status });
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const feed = sseParser((data) => {
      const frame = decodeServerFrame(data);
      if (frame !== undefined) events.frame(frame);
    });
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      events.rx();
      feed(decoder.decode(value, { stream: true }));
    }
    events.closed({});
  };
  const end = (): void => {
    if (!control.signal.aborted) events.closed({});
  };
  run().then(undefined, end);
  return { send: () => false, close: () => control.abort() };
}

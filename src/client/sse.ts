import { RealtimeError } from "../errors";
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

/**
 * Splits an event stream into `rt` events; keeps a partial line (and a trailing CR) for the next chunk.
 *
 * What the parser holds is bounded: the unfinished line plus the `data` lines of the event being read may not pass `maxBuffer`
 * code units (default 1 MiB, far above the largest frame a hub sends with its default limits; the cap is a function so a
 * hub configured with larger frames can raise it once its `ready` frame has said so). A server that streams an endless line, or endless
 * `data:` lines with no blank line, would otherwise grow the client's memory without limit; the feed function throws a
 * `RealtimeError` with code `too_large` instead, and the transport closes and reconnects.
 * @internal
 */
export function sseParser(
  onData: (data: string) => void,
  maxBuffer: () => number = () => 1_048_576,
): (chunk: string) => void {
  let buffer = "";
  let name = "";
  // The `data` lines of the event being read, joined by newlines; undefined until the event has one.
  let data: string | undefined;
  return (chunk) => {
    // A line ends at LF, CRLF or CR; a CR at the very end waits for the next chunk, which may start with the LF.
    const lines = (buffer + chunk).split(/\r\n|\n|\r(?!$)/);
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (line === "") {
        if (name === "rt" && data !== undefined) onData(data);
        name = "";
        data = undefined;
        continue;
      }
      // `field`, `field:value` or `field: value` (one space after the colon is dropped). A comment line has an empty
      // field name, which matches neither branch. The pattern always matches (every group may be empty), and its `s`
      // flag keeps U+2028 in a value: only CR and LF end a line.
      const [, key, value] = /^([^:]*):? ?(.*)/s.exec(line) as unknown as [string, string, string];
      if (key === "event") name = value;
      else if (key === "data") data = data === undefined ? value : `${data}\n${value}`;
    }
    if (buffer.length + (data?.length ?? 0) > maxBuffer()) {
      buffer = "";
      data = undefined;
      throw new RealtimeError(
        "too_large",
        __DEV__ ? "the event stream sent more than the client buffers" : undefined,
      );
    }
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
    // A hub may be configured with frames larger than the default cap: its `ready` frame names the largest it sends.
    let cap = 1_048_576;
    const feed = sseParser(
      (data) => {
        const frame = decodeServerFrame(data);
        if (frame === undefined) return;
        if (frame.t === "ready") cap = Math.min(frame.max * 2 + 1_048_576, 16_777_216);
        events.frame(frame);
      },
      () => cap,
    );
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      events.rx();
      // An oversized stream throws here, which stops the reading; `end` aborts the request and reports the close.
      feed(decoder.decode(value, { stream: true }));
    }
    events.closed({});
  };
  const end = (): void => {
    if (control.signal.aborted) return;
    control.abort();
    events.closed({});
  };
  run().then(undefined, end);
  return { send: () => false, close: () => control.abort() };
}

import { decodeServerFrame } from "../protocol";
import type { Link, LinkEvents } from "./sse";

/** Opens a WebSocket whose text messages are protocol frames. Throws for a URL the constructor refuses. @internal */
export function openSocket(url: string, Socket: typeof WebSocket, events: LinkEvents): Link {
  const socket = new Socket(url);
  socket.onmessage = (event: MessageEvent<unknown>) => {
    if (typeof event.data !== "string") return;
    events.rx();
    const frame = decodeServerFrame(event.data);
    if (frame !== undefined) events.frame(frame);
  };
  // A failed upgrade is `error` then `close` (code 1006) in browsers; Node 22's WebSocket fires only `error` and stays connecting, so
  // an error before the socket opened counts as the close, once.
  let ended = false;
  const end = (code: number): void => {
    if (ended) return;
    ended = true;
    events.closed({ code });
  };
  socket.onclose = (event: CloseEvent) => end(event.code);
  socket.onerror = () => {
    if (socket.readyState !== 1) end(1006);
  };
  return {
    send(text) {
      if (socket.readyState !== 1) return false;
      socket.send(text);
      return true;
    },
    close() {
      socket.onmessage = null;
      socket.onclose = null;
      socket.onerror = null;
      socket.close(1000);
    },
  };
}

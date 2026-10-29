/** The delay before reconnect attempt `attempt` (0-based): full jitter below an exponential cap. @internal */
export function backoff(attempt: number, min: number, max: number, random: () => number): number {
  return random() * Math.min(max, min * 2 ** Math.min(attempt, 30));
}

/** A random identifier of 12 base-36 characters, used to build idempotency keys. @internal */
export function clientId(): string {
  // Six bytes of two base-36 characters each make the twelve.
  const bytes = new Uint8Array(6);
  globalThis.crypto.getRandomValues(bytes);
  let id = "";
  for (const byte of bytes) id += byte.toString(36).padStart(2, "0");
  return id;
}

/** Resolves a stream or POST path against the page; `ws` swaps `http(s):` for `ws(s):`. @internal */
export function resolveUrl(url: string, ws: boolean): URL {
  const href = globalThis.location?.href;
  const base = href !== undefined && URL.canParse(url, href) ? href : "http://localhost/";
  const resolved = new URL(url, base);
  if (ws) resolved.protocol = resolved.protocol === "https:" ? "wss:" : "ws:";
  return resolved;
}

/** The default POST URL: the stream path plus `/send`, keeping the query. @internal */
export function sendUrl(url: URL): URL {
  const next = new URL(url);
  next.pathname = `${next.pathname.replace(/\/$/, "")}/send`;
  return next;
}

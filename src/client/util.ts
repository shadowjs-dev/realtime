/** The delay before reconnect attempt `attempt` (0-based): full jitter below an exponential cap. @internal */
export function backoff(attempt: number, min: number, max: number, random: () => number): number {
  return random() * Math.min(max, min * 2 ** Math.min(attempt, 30));
}

/** Resolves a stream or POST path against the page; `ws` swaps `http(s):` for `ws(s):`. @internal */
export function resolveUrl(url: string, ws: boolean): URL {
  const href = globalThis.location?.href;
  const base = href !== undefined && URL.canParse(url, href) ? href : "http://localhost/";
  const resolved = new URL(url, base);
  if (ws) resolved.protocol = resolved.protocol === "https:" ? "wss:" : "ws:";
  return resolved;
}

/**
 * The limits of a hub. Every one is bounded, so a hub cannot be made to hold unbounded state (see the realtime protocol reference).
 * @since 1.0.0
 */
export interface HubLimits {
  /** Largest frame, in UTF-8 bytes, either way. Default 65,536. */
  readonly maxMessageBytes: number;
  /** Channels one connection may subscribe to. Default 64. */
  readonly maxChannelsPerConnection: number;
  /** Subscribers one channel may have on this server. Default 50,000. */
  readonly maxSubscribersPerChannel: number;
  /** Presence members one channel may have, across servers. Default 1,000. */
  readonly maxPresenceMembers: number;
  /** Largest presence state, in bytes of JSON. Default 1,024. */
  readonly maxStateBytes: number;
  /** Requests one connection may have in flight. Default 32. */
  readonly maxPendingRequests: number;
  /** Frames one connection may have waiting to be handled. Default 256. */
  readonly maxQueuedFrames: number;
  /** Bytes the transport may hold unsent before the connection is closed as a slow consumer. Default 1 MiB. */
  readonly maxBufferedBytes: number;
  /**
   * Publishes per second per connection. Default 20. It is also the refill rate of the bucket that `sub` and `unsub` frames spend
   * from (burst: the larger of `publishBurst` and `maxChannelsPerConnection`), so subscription churn is limited like publishing.
   */
  readonly publishesPerSecond: number;
  /** Largest burst of publishes per connection. Default 40. */
  readonly publishBurst: number;
  /** Publishes per second per channel on this server. Default 500. */
  readonly channelPublishesPerSecond: number;
  /** Milliseconds a request may run. Default 10,000. */
  readonly requestTimeoutMs: number;
  /** Messages kept per channel unless a rule says otherwise. Default 100. */
  readonly history: number;
  /** Milliseconds a message is kept. Default 300,000. */
  readonly historyTtlMs: number;
  /** Milliseconds an idempotency key is remembered. Default 60,000. */
  readonly idempotencyTtlMs: number;
  /** Milliseconds a presence member lives without a heartbeat. Default 45,000. */
  readonly presenceTtlMs: number;
  /** Milliseconds between the heartbeats a client is asked to send (`ready.hb`). Default 30,000. */
  readonly heartbeatMs: number;
  /** Milliseconds between presence heartbeats (and reaping). Default 15,000. */
  readonly presenceHeartbeatMs: number;
}

/** The defaults of {@link HubLimits}. @since 1.0.0 */
export const DEFAULT_HUB_LIMITS: HubLimits = Object.freeze({
  maxMessageBytes: 65_536,
  maxChannelsPerConnection: 64,
  maxSubscribersPerChannel: 50_000,
  maxPresenceMembers: 1_000,
  maxStateBytes: 1_024,
  maxPendingRequests: 32,
  maxQueuedFrames: 256,
  maxBufferedBytes: 1_048_576,
  publishesPerSecond: 20,
  publishBurst: 40,
  channelPublishesPerSecond: 500,
  requestTimeoutMs: 10_000,
  history: 100,
  historyTtlMs: 300_000,
  idempotencyTtlMs: 60_000,
  presenceTtlMs: 45_000,
  presenceHeartbeatMs: 15_000,
  heartbeatMs: 30_000,
});

/** The most messages a channel may retain. */
export const MAX_HISTORY = 10_000;

/**
 * Validates and fills in the limits.
 * @param given The limits the app set.
 * @returns The complete limits.
 * @throws RangeError for a limit that is not a positive integer (a history of 0 is allowed).
 */
export function resolveLimits(given: Partial<HubLimits> | undefined): HubLimits {
  const out: Record<string, number> = { ...DEFAULT_HUB_LIMITS };
  for (const [name, value] of Object.entries(given ?? {})) {
    if (!(name in DEFAULT_HUB_LIMITS))
      throw new TypeError(__DEV__ ? `unknown limit "${name}".` : "REALTIME_LIMIT_2");
    if (value === undefined) continue;
    const floor = name === "history" ? 0 : 1;
    if (typeof value !== "number" || !Number.isInteger(value) || value < floor)
      throw new RangeError(
        __DEV__
          ? `limits.${name} must be ${floor === 0 ? "a non-negative" : "a positive"} integer.`
          : "REALTIME_LIMIT_4",
      );
    out[name] = value;
  }
  if ((out["history"] as number) > MAX_HISTORY)
    throw new RangeError(
      __DEV__ ? `limits.history must be at most ${MAX_HISTORY}.` : "REALTIME_LIMIT_1",
    );
  if ((out["presenceTtlMs"] as number) <= (out["presenceHeartbeatMs"] as number))
    throw new RangeError(
      __DEV__
        ? "limits.presenceTtlMs must be larger than limits.presenceHeartbeatMs."
        : "REALTIME_LIMIT_3",
    );
  return out as unknown as HubLimits;
}

/** A token bucket: `rate` tokens per second up to `burst`. */
export class TokenBucket {
  #tokens: number;
  #at: number;
  readonly #rate: number;
  readonly #burst: number;

  constructor(rate: number, burst: number, now: number) {
    this.#rate = rate;
    this.#burst = burst;
    this.#tokens = burst;
    this.#at = now;
  }

  /** Takes one token; false when the bucket is empty. */
  take(now: number): boolean {
    const elapsed = Math.max(0, now - this.#at);
    this.#at = now;
    this.#tokens = Math.min(this.#burst, this.#tokens + (elapsed * this.#rate) / 1000);
    if (this.#tokens < 1) return false;
    this.#tokens -= 1;
    return true;
  }
}

/** Counts events inside a sliding window; `hit` returns true once the count reaches the limit. */
export class WindowCounter {
  readonly #times: number[] = [];
  readonly #limit: number;
  readonly #windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.#limit = limit;
    this.#windowMs = windowMs;
  }

  hit(now: number): boolean {
    this.#times.push(now);
    const cutoff = now - this.#windowMs;
    while (this.#times.length > 0 && (this.#times[0] as number) <= cutoff) this.#times.shift();
    return this.#times.length >= this.#limit;
  }
}

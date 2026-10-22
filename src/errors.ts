/**
 * The codes an error of this package carries. They are the same strings the wire protocol uses in `err` frames.
 * @since 1.0.0
 */
export type ErrorCode =
  | "bad_frame"
  | "bad_channel"
  | "unauthorized"
  | "forbidden"
  | "not_subscribed"
  | "too_large"
  | "rate_limited"
  | "limit"
  | "timeout"
  | "unavailable"
  | "no_handler"
  | "handler_error"
  | "closed"
  | "queue_full"
  | "conflict";

/**
 * An error with a protocol code. A request handler that throws one answers the client with its code and message; any
 * other error answers `handler_error` with no message, so internals never reach a client.
 *
 * @example
 * import { RealtimeError } from "@shadow-js/realtime";
 *
 * const refusal = new RealtimeError("forbidden", "only moderators can pin messages");
 * refusal.code; // "forbidden"
 * @since 1.0.0
 */
export class RealtimeError extends Error {
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message?: string, options?: { readonly cause?: unknown }) {
    super(message ?? code, options);
    this.name = "RealtimeError";
    this.code = code;
  }
}

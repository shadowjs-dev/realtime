/**
 * The private protocol between the adapter (in a Worker) and the channel object (a Durable Object): JSON requests to
 * `/v1/<op>` and the frames of the subscribe WebSocket. Both sides validate what they read; neither trusts the other's shape.
 * @packageDocumentation
 */
import type { PublishOptions } from "../adapter";
import type { ErrorCode } from "../errors";
import {
  byteLength,
  isChannelName,
  isEventName,
  isIdempotencyKey,
  type Json,
  type Member,
  type PresenceEvent,
  type StoredEntry,
  type StoredMessage,
} from "../protocol";

/** The version in the request path. @internal */
export const WIRE_VERSION: number = 1;

/** The most bytes of one message (data, event and names) the object stores in one value; the platform's smallest value limit is 128 KiB. @internal */
export const MAX_ENTRY_BYTES: number = 120 * 1024;
/** The most bytes of one member's state. @internal */
export const MAX_STATE_BYTES: number = 16 * 1024;
/** The most members one channel object keeps. @internal */
export const MAX_MEMBERS: number = 10_000;
/** The most messages one channel retains. @internal */
export const MAX_HISTORY: number = 10_000;
/** The longest expiry of a presence member or an idempotency key. @internal */
export const MAX_TTL_MS: number = 86_400_000;
/** The most bytes of a request body the object reads. @internal */
export const MAX_BODY_BYTES: number = 256 * 1024;

/** The operations of the request protocol. @internal */
export type Operation =
  | "publish"
  | "history"
  | "presence-set"
  | "presence-remove"
  | "presence-list"
  | "presence-touch"
  | "presence-reap";

/** A parsed request. @internal */
export type ParsedRequest =
  | {
      readonly op: "publish";
      readonly channel: string;
      readonly entry: StoredEntry;
      readonly options: PublishOptions;
      readonly idleMs: number;
    }
  | {
      readonly op: "history";
      readonly channel: string;
      readonly after: number;
      readonly limit: number;
    }
  | {
      readonly op: "presence-set";
      readonly channel: string;
      readonly member: Member;
      readonly ttlMs: number;
      readonly graceMs: number;
    }
  | { readonly op: "presence-remove"; readonly channel: string; readonly id: string }
  | { readonly op: "presence-list"; readonly channel: string }
  | {
      readonly op: "presence-touch";
      readonly channel: string;
      readonly ids: readonly string[];
      readonly ttlMs: number;
    }
  | { readonly op: "presence-reap"; readonly channel: string };

/** What parsing a request yields. @internal */
export type Parsed =
  | { readonly ok: true; readonly request: ParsedRequest }
  | { readonly ok: false; readonly code: ErrorCode; readonly message: string };

/** A page of history as the object returns it. @internal */
export interface HistoryPage {
  readonly messages: readonly StoredMessage[];
  readonly head: number;
  readonly oldest: number;
  /** True when retained messages after the last one returned remain. */
  readonly more: boolean;
}

/** Frames the object sends on a subscribe socket. @internal */
export type LinkFrame =
  | { readonly t: "ready"; readonly head: number; readonly oldest: number }
  | { readonly t: "snap"; readonly members: readonly Member[] }
  | { readonly t: "msg"; readonly m: StoredMessage }
  | {
      readonly t: "pres";
      readonly op: PresenceEvent["op"];
      readonly member: Member;
    }
  | { readonly t: "more" }
  | { readonly t: "bye"; readonly why: string };

const OPERATIONS: ReadonlySet<string> = new Set<Operation>([
  "publish",
  "history",
  "presence-set",
  "presence-remove",
  "presence-list",
  "presence-touch",
  "presence-reap",
]);

/** Whether `value` names an operation. @internal */
export function isOperation(value: string): value is Operation {
  return OPERATIONS.has(value);
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCount(value: unknown, max = Number.MAX_SAFE_INTEGER): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= max;
}

function fail(message: string, code: ErrorCode = "bad_frame"): Parsed {
  return { ok: false, code, message };
}

/**
 * Whether `value` is a well formed member: an id, an optional user, a version of 1 or more and a JSON state of bounded size.
 * @internal
 */
export function parseMember(value: unknown): Member | undefined {
  if (!isRecord(value)) return undefined;
  const { id, u, s, v } = value;
  if (typeof id !== "string" || id.length === 0 || id.length > 256) return undefined;
  if (u !== undefined && (typeof u !== "string" || u.length > 256)) return undefined;
  if (typeof v !== "number" || !Number.isInteger(v) || v < 1) return undefined;
  const state: Json = s === undefined ? null : (s as Json);
  const text = JSON.stringify(state);
  if (text === undefined || byteLength(text) > MAX_STATE_BYTES) return undefined;
  return u === undefined ? { id, s: state, v } : { id, u, s: state, v };
}

function parseEntry(value: unknown): StoredEntry | undefined {
  if (!isRecord(value)) return undefined;
  const { event, data, publisher, user } = value;
  if (!isEventName(event) || typeof data !== "string") return undefined;
  if (publisher !== undefined && (typeof publisher !== "string" || publisher.length > 256))
    return undefined;
  if (user !== undefined && (typeof user !== "string" || user.length > 256)) return undefined;
  return {
    event,
    data,
    ...(publisher === undefined ? {} : { publisher }),
    ...(user === undefined ? {} : { user }),
  };
}

function parseOptions(value: unknown): PublishOptions | undefined {
  if (!isRecord(value)) return undefined;
  const { history, historyTtlMs, key, idempotencyTtlMs } = value;
  if (!isCount(history, MAX_HISTORY)) return undefined;
  if (!isCount(historyTtlMs, MAX_TTL_MS * 366)) return undefined;
  if (!isCount(idempotencyTtlMs, MAX_TTL_MS)) return undefined;
  if (key !== undefined && !isIdempotencyKey(key)) return undefined;
  return key === undefined
    ? { history, historyTtlMs, idempotencyTtlMs }
    : { history, historyTtlMs, key, idempotencyTtlMs };
}

/**
 * Parses the JSON body of a request to operation `op`.
 *
 * @param op The operation from the path.
 * @param body The parsed JSON body.
 * @returns The request, or the refusal to answer.
 * @internal
 */
export function parseRequest(op: Operation, body: unknown): Parsed {
  if (!isRecord(body)) return fail("the body must be an object");
  const { channel } = body;
  if (!isChannelName(channel)) return fail("bad channel name", "bad_channel");
  switch (op) {
    case "publish": {
      const entry = parseEntry(body["entry"]);
      const options = parseOptions(body["options"]);
      const idleMs = body["idleMs"] ?? 0;
      if (entry === undefined) return fail("bad entry");
      if (options === undefined) return fail("bad options");
      if (!isCount(idleMs, MAX_TTL_MS * 3650)) return fail("bad idleMs");
      if (byteLength(entry.data) + byteLength(entry.event) > MAX_ENTRY_BYTES)
        return fail("the message is too large for one stored value", "too_large");
      return { ok: true, request: { op, channel, entry, options, idleMs } };
    }
    case "history": {
      const { after, limit } = body;
      if (!isCount(after) || !isCount(limit, 100_000)) return fail("bad after or limit");
      return { ok: true, request: { op, channel, after, limit } };
    }
    case "presence-set": {
      const member = parseMember(body["member"]);
      const { ttlMs } = body;
      const graceMs = body["graceMs"] ?? 0;
      if (member === undefined) return fail("bad member");
      if (!isCount(ttlMs, MAX_TTL_MS) || ttlMs === 0) return fail("bad ttlMs");
      if (!isCount(graceMs, 3_600_000)) return fail("bad graceMs");
      return { ok: true, request: { op, channel, member, ttlMs, graceMs } };
    }
    case "presence-remove": {
      const { id } = body;
      if (typeof id !== "string" || id.length === 0 || id.length > 256) return fail("bad id");
      return { ok: true, request: { op, channel, id } };
    }
    case "presence-touch": {
      const { ids, ttlMs } = body;
      if (!isCount(ttlMs, MAX_TTL_MS) || ttlMs === 0) return fail("bad ttlMs");
      if (
        !Array.isArray(ids) ||
        ids.length > MAX_MEMBERS ||
        !ids.every((id) => typeof id === "string" && id.length > 0 && id.length <= 256)
      )
        return fail("bad ids");
      return { ok: true, request: { op, channel, ids: ids as string[], ttlMs } };
    }
    case "presence-list":
    case "presence-reap":
      return { ok: true, request: { op, channel } };
  }
}

/** Decodes a frame from the object; `undefined` for anything that is not a known, well formed frame. @internal */
export function decodeLinkFrame(text: string): LinkFrame | undefined {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!isRecord(value)) return undefined;
  switch (value["t"]) {
    case "ready": {
      const { head, oldest } = value;
      return isCount(head) && isCount(oldest) ? { t: "ready", head, oldest } : undefined;
    }
    case "snap": {
      const list = value["members"];
      if (!Array.isArray(list)) return undefined;
      const members: Member[] = [];
      for (const item of list) {
        const member = parseMember(item);
        if (member === undefined) return undefined;
        members.push(member);
      }
      return { t: "snap", members };
    }
    case "msg": {
      const message = parseStored(value["m"]);
      return message === undefined ? undefined : { t: "msg", m: message };
    }
    case "pres": {
      const member = parseMember(value["member"]);
      const op = value["op"];
      if (member === undefined || (op !== "join" && op !== "set" && op !== "leave"))
        return undefined;
      return { t: "pres", op, member };
    }
    case "more":
      return { t: "more" };
    case "bye":
      return { t: "bye", why: typeof value["why"] === "string" ? value["why"] : "" };
    default:
      return undefined;
  }
}

/** Reads a stored message; `undefined` when it is malformed. @internal */
export function parseStored(value: unknown): StoredMessage | undefined {
  if (!isRecord(value)) return undefined;
  const { seq, event, data, at, publisher, user } = value;
  if (!isCount(seq) || seq < 1 || typeof event !== "string" || typeof data !== "string")
    return undefined;
  if (!isCount(at)) return undefined;
  if (publisher !== undefined && typeof publisher !== "string") return undefined;
  if (user !== undefined && typeof user !== "string") return undefined;
  return {
    seq,
    event,
    data,
    at,
    ...(publisher === undefined ? {} : { publisher }),
    ...(user === undefined ? {} : { user }),
  };
}

/** Whether `value` is a count (a non-negative safe integer). @internal */
export function isWholeNumber(value: unknown): value is number {
  return isCount(value);
}

/** The key of retained message `seq`: zero padded so keys sort by seq. @internal */
export function historyKey(seq: number): string {
  return `h:${String(seq).padStart(16, "0")}`;
}

/** The seq a history key stands for. @internal */
export function seqOfKey(key: string): number {
  return Number(key.slice(2));
}

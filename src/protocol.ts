import type { ErrorCode } from "./errors";

/**
 * The wire protocol version this package speaks (https://realtime.shadowjs.net/guides/realtime-protocol/).
 * @example
 * import { PROTOCOL_VERSION } from "@shadow-js/realtime";
 *
 * const hello = JSON.stringify({ t: "hello", v: PROTOCOL_VERSION });
 * @since 1.0.0
 */
export const PROTOCOL_VERSION: 1 = 1;

/**
 * Any JSON value.
 * @since 1.0.0
 */
export type Json =
  | null
  | boolean
  | number
  | string
  | readonly Json[]
  | { readonly [key: string]: Json };

/**
 * One member of a channel's presence set: the connection that joined, its user, its state, and the version of that state.
 * @since 1.0.0
 */
export interface Member<State = Json> {
  /** The connection id. */
  readonly id: string;
  /** The user of the connection's identity, when it has one. */
  readonly u?: string;
  /** The member's state. */
  readonly s: State;
  /** A counter that rises with every state change of this member, from 1. */
  readonly v: number;
}

/** A message as stored by an adapter: the data is its JSON text. @since 1.0.0 */
export interface StoredMessage {
  readonly seq: number;
  readonly event: string;
  /** The JSON text of the data. */
  readonly data: string;
  /** The publishing connection id, when a connection published it. */
  readonly publisher?: string;
  /** The publishing user, when it has one. */
  readonly user?: string;
  /** When the adapter accepted it, in epoch milliseconds. */
  readonly at: number;
}

/** What a publisher supplies; the adapter adds `seq` and `at`. @since 1.0.0 */
export type StoredEntry = Omit<StoredMessage, "seq" | "at">;

/** A change of a presence set, as adapters deliver it. @since 1.0.0 */
export interface PresenceEvent {
  readonly channel: string;
  readonly op: "join" | "set" | "leave";
  readonly member: Member;
}

/** Frames a client sends. @since 1.0.0 */
export type ClientFrame =
  | { readonly t: "hello"; readonly v: number }
  | {
      readonly t: "sub";
      readonly n: number;
      readonly c: string;
      readonly after?: number;
      readonly echo?: boolean;
      readonly presence?: Json;
      readonly hasPresence: boolean;
      readonly watch?: boolean;
    }
  | { readonly t: "unsub"; readonly n?: number; readonly c: string }
  | {
      readonly t: "pub";
      readonly c: string;
      readonly e: string;
      readonly d: string;
      readonly n?: number;
      readonly k?: string;
    }
  | {
      readonly t: "req";
      readonly n: number;
      readonly c: string;
      readonly e: string;
      readonly d: Json;
      readonly ms?: number;
    }
  | { readonly t: "pres"; readonly c: string; readonly s: Json }
  | { readonly t: "ping"; readonly n?: number };

/** What decoding a client frame yields. @since 1.0.0 */
export type DecodedClientFrame =
  | { readonly ok: true; readonly frame: ClientFrame }
  | { readonly ok: false; readonly code: ErrorCode; readonly n?: number; readonly why: string };

const CHANNEL = /^[A-Za-z0-9_.:/@-]{1,128}$/;
const EVENT = /^[A-Za-z0-9_.:@-]{1,64}$/;
const KEY = /^[A-Za-z0-9_.:@-]{1,64}$/;
const encoder = new TextEncoder();

/**
 * Whether `value` is a valid channel name: 1..128 characters of `A-Za-z0-9_.:/@-`, not starting with the reserved `$`.
 * @param value The candidate.
 * @returns True for a valid name.
 * @example
 * import { isChannelName } from "@shadow-js/realtime";
 *
 * isChannelName("chat:lobby"); // true
 * isChannelName("chat lobby"); // false
 * @since 1.0.0
 */
export function isChannelName(value: unknown): value is string {
  return typeof value === "string" && CHANNEL.test(value);
}

/**
 * Whether `value` is a valid event name: 1..64 characters of `A-Za-z0-9_.:@-`.
 * @param value The candidate.
 * @returns True for a valid name.
 * @example
 * import { isEventName } from "@shadow-js/realtime";
 *
 * isEventName("message"); // true
 * isEventName("a/b"); // false
 * @since 1.0.0
 */
export function isEventName(value: unknown): value is string {
  return typeof value === "string" && EVENT.test(value);
}

/** Whether `value` is a valid idempotency key. @internal */
export function isIdempotencyKey(value: unknown): value is string {
  return typeof value === "string" && KEY.test(value);
}

/** The number of bytes of `text` in UTF-8, without encoding the common cases. @internal */
export function byteLength(text: string): number {
  // Every UTF-16 unit is at most 3 bytes in UTF-8, and at least 1.
  if (text.length * 3 <= 0x7fffffff) {
    let ascii = true;
    for (let at = 0; at < text.length; at += 1) {
      if (text.charCodeAt(at) > 0x7f) {
        ascii = false;
        break;
      }
    }
    if (ascii) return text.length;
  }
  return encoder.encode(text).length;
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

type Refusal = Extract<DecodedClientFrame, { ok: false }>;

function bad(why: string, n?: unknown): Refusal {
  return isCount(n)
    ? { ok: false, code: "bad_frame", n, why }
    : { ok: false, code: "bad_frame", why };
}

/**
 * Decodes and validates one client frame (server side). Never throws: any text yields a frame or a refusal.
 *
 * @param text The frame text.
 * @param maxBytes The largest accepted frame, in UTF-8 bytes.
 * @returns The frame, or the code and reason to refuse it with.
 * @example
 * import { decodeClientFrame } from "@shadow-js/realtime";
 *
 * const result = decodeClientFrame('{"t":"ping"}', 65_536);
 * result.ok; // true
 * @since 1.0.0
 */
export function decodeClientFrame(text: string, maxBytes: number): DecodedClientFrame {
  if (byteLength(text) > maxBytes)
    return { ok: false, code: "too_large", why: "the frame is larger than the limit" };
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return bad("the frame is not JSON");
  }
  if (value === null || typeof value !== "object" || Array.isArray(value))
    return bad("the frame is not an object");
  const raw = value as Record<string, unknown>;
  const type = raw["t"];
  const n = raw["n"];
  if (n !== undefined && !isCount(n)) return bad("n must be a non-negative integer");
  const c = raw["c"];
  const needChannel = (): Refusal | undefined =>
    isChannelName(c)
      ? undefined
      : { ...bad("c is not a valid channel name", n), code: "bad_channel" };
  switch (type) {
    case "hello": {
      const v = raw["v"];
      if (typeof v !== "number" || !Number.isInteger(v) || v < 1) return bad("v must be a version");
      return { ok: true, frame: { t: "hello", v } };
    }
    case "ping":
      return { ok: true, frame: n === undefined ? { t: "ping" } : { t: "ping", n } };
    case "sub": {
      const refusal = needChannel();
      if (refusal !== undefined) return refusal;
      if (n === undefined) return bad("sub needs n");
      const after = raw["after"];
      if (after !== undefined && !isCount(after))
        return bad("after must be a non-negative integer", n);
      const echo = raw["echo"];
      if (echo !== undefined && typeof echo !== "boolean") return bad("echo must be a boolean", n);
      const watch = raw["watch"];
      if (watch !== undefined && typeof watch !== "boolean")
        return bad("watch must be a boolean", n);
      const hasPresence = "presence" in raw;
      return {
        ok: true,
        frame: {
          t: "sub",
          n,
          c: c as string,
          hasPresence,
          ...(after === undefined ? {} : { after }),
          ...(echo === undefined ? {} : { echo }),
          ...(watch === undefined ? {} : { watch }),
          ...(hasPresence ? { presence: raw["presence"] as Json } : {}),
        },
      };
    }
    case "unsub": {
      const refusal = needChannel();
      if (refusal !== undefined) return refusal;
      return {
        ok: true,
        frame: n === undefined ? { t: "unsub", c: c as string } : { t: "unsub", n, c: c as string },
      };
    }
    case "pub": {
      const refusal = needChannel();
      if (refusal !== undefined) return refusal;
      const e = raw["e"];
      if (!isEventName(e)) return bad("e is not a valid event name", n);
      const k = raw["k"];
      if (k !== undefined && !isIdempotencyKey(k)) return bad("k is not a valid key", n);
      const data = JSON.stringify(raw["d"] ?? null);
      return {
        ok: true,
        frame: {
          t: "pub",
          c: c as string,
          e,
          d: data,
          ...(n === undefined ? {} : { n }),
          ...(k === undefined ? {} : { k }),
        },
      };
    }
    case "req": {
      const refusal = needChannel();
      if (refusal !== undefined) return refusal;
      if (n === undefined) return bad("req needs n");
      const e = raw["e"];
      if (!isEventName(e)) return bad("e is not a valid event name", n);
      const ms = raw["ms"];
      if (ms !== undefined && !(typeof ms === "number" && Number.isFinite(ms) && ms > 0))
        return bad("ms must be a positive number", n);
      return {
        ok: true,
        frame: {
          t: "req",
          n,
          c: c as string,
          e,
          d: (raw["d"] ?? null) as Json,
          ...(ms === undefined ? {} : { ms }),
        },
      };
    }
    case "pres": {
      const refusal = needChannel();
      if (refusal !== undefined) return refusal;
      return { ok: true, frame: { t: "pres", c: c as string, s: (raw["s"] ?? null) as Json } };
    }
    default:
      return bad("unknown frame type", n);
  }
}

/** Frames a server sends, as a client reads them. @since 1.0.0 */
export type ServerFrame =
  | {
      readonly t: "ready";
      readonly v: number;
      readonly id: string;
      readonly hb: number;
      readonly max: number;
    }
  | {
      readonly t: "subbed";
      readonly n: number;
      readonly c: string;
      readonly head: number;
      readonly gap?: boolean;
      readonly reset?: boolean;
    }
  | { readonly t: "unsubbed"; readonly c: string; readonly why?: string }
  | {
      readonly t: "msg";
      readonly c: string;
      readonly s: number;
      readonly e: string;
      readonly d: Json;
      readonly p?: string;
      readonly u?: string;
      readonly r?: 1;
    }
  | { readonly t: "ack"; readonly n: number; readonly s: number; readonly dup?: boolean }
  | { readonly t: "rep"; readonly n: number; readonly d: Json }
  | {
      readonly t: "err";
      readonly n?: number;
      readonly c?: string;
      readonly code: ErrorCode;
      readonly msg?: string;
    }
  | {
      readonly t: "pres";
      readonly c: string;
      readonly op: "sync" | "join" | "set" | "leave";
      readonly m: Member | readonly Member[];
    }
  | { readonly t: "pong"; readonly n?: number }
  | { readonly t: "bye"; readonly code: number; readonly why: string };

/**
 * Decodes one server frame (client side). Returns `undefined` for anything that is not a known, well-formed frame, so a
 * client ignores what a newer server may send and never acts on a malformed frame.
 *
 * @param text The frame text.
 * @returns The frame, or undefined.
 * @example
 * import { decodeServerFrame } from "@shadow-js/realtime";
 *
 * decodeServerFrame('{"t":"pong"}'); // { t: "pong" }
 * decodeServerFrame("not json"); // undefined
 * @since 1.0.0
 */
export function decodeServerFrame(text: string): ServerFrame | undefined {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const raw = value as Record<string, unknown>;
  const str = (key: string): string | undefined => {
    const item = raw[key];
    return typeof item === "string" ? item : undefined;
  };
  const num = (key: string): number | undefined => {
    const item = raw[key];
    return isCount(item) ? item : undefined;
  };
  switch (raw["t"]) {
    case "ready": {
      const v = num("v");
      const id = str("id");
      if (v === undefined || id === undefined) return undefined;
      return { t: "ready", v, id, hb: num("hb") ?? 30_000, max: num("max") ?? 65_536 };
    }
    case "subbed": {
      const n = num("n");
      const c = str("c");
      const head = num("head");
      if (n === undefined || c === undefined || head === undefined) return undefined;
      return {
        t: "subbed",
        n,
        c,
        head,
        ...(raw["gap"] === true ? { gap: true } : {}),
        ...(raw["reset"] === true ? { reset: true } : {}),
      };
    }
    case "unsubbed": {
      const c = str("c");
      if (c === undefined) return undefined;
      const why = str("why");
      return why === undefined ? { t: "unsubbed", c } : { t: "unsubbed", c, why };
    }
    case "msg": {
      const c = str("c");
      const s = num("s");
      const e = str("e");
      if (c === undefined || s === undefined || e === undefined || s < 1) return undefined;
      const p = str("p");
      const u = str("u");
      return {
        t: "msg",
        c,
        s,
        e,
        d: raw["d"] as Json,
        ...(p === undefined ? {} : { p }),
        ...(u === undefined ? {} : { u }),
        ...(raw["r"] === 1 ? { r: 1 as const } : {}),
      };
    }
    case "ack": {
      const n = num("n");
      const s = num("s");
      if (n === undefined || s === undefined) return undefined;
      return { t: "ack", n, s, ...(raw["dup"] === true ? { dup: true } : {}) };
    }
    case "rep": {
      const n = num("n");
      if (n === undefined) return undefined;
      return { t: "rep", n, d: raw["d"] as Json };
    }
    case "err": {
      const code = str("code");
      if (code === undefined) return undefined;
      const n = num("n");
      const c = str("c");
      const msg = str("msg");
      return {
        t: "err",
        code: code as ErrorCode,
        ...(n === undefined ? {} : { n }),
        ...(c === undefined ? {} : { c }),
        ...(msg === undefined ? {} : { msg }),
      };
    }
    case "pres": {
      const c = str("c");
      const op = raw["op"];
      const m = raw["m"];
      if (c === undefined || (op !== "sync" && op !== "join" && op !== "set" && op !== "leave"))
        return undefined;
      const member = (item: unknown): Member | undefined => {
        if (item === null || typeof item !== "object") return undefined;
        const entry = item as Record<string, unknown>;
        if (typeof entry["id"] !== "string" || !isCount(entry["v"])) return undefined;
        return {
          id: entry["id"],
          v: entry["v"],
          s: (entry["s"] ?? null) as Json,
          ...(typeof entry["u"] === "string" ? { u: entry["u"] } : {}),
        };
      };
      if (op === "sync") {
        if (!Array.isArray(m)) return undefined;
        const list = m.map(member);
        if (list.some((item) => item === undefined)) return undefined;
        return { t: "pres", c, op, m: list as Member[] };
      }
      const one = member(m);
      return one === undefined ? undefined : { t: "pres", c, op, m: one };
    }
    case "pong": {
      const n = num("n");
      return n === undefined ? { t: "pong" } : { t: "pong", n };
    }
    case "bye": {
      const code = num("code");
      return code === undefined ? undefined : { t: "bye", code, why: str("why") ?? "" };
    }
    default:
      return undefined;
  }
}

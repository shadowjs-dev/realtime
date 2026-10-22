import type { Json } from "../protocol";

/** Who a connection is. `user` identifies a person; other claims are the app's. @since 1.0.0 */
export interface Identity {
  readonly user?: string;
  readonly [claim: string]: unknown;
}

/** What a rule predicate is asked. @since 1.0.0 */
export interface RuleContext<
  Params extends Readonly<Record<string, string>> = Record<string, string>,
> {
  readonly channel: string;
  readonly params: Params;
  readonly identity: Identity;
  readonly op: "subscribe" | "publish" | "presence";
}

/** `true`, `false`, or a predicate that decides per connection. @since 1.0.0 */
export type Permission<Params extends Readonly<Record<string, string>> = Record<string, string>> =
  | boolean
  | ((context: RuleContext<Params>) => boolean | Promise<boolean>);

/** What a request handler is given. @since 1.0.0 */
export interface RequestContext<
  Params extends Readonly<Record<string, string>> = Record<string, string>,
> {
  readonly channel: string;
  readonly params: Params;
  readonly identity: Identity;
  readonly event: string;
  readonly data: Json;
  /** Aborted when the connection closes, the request times out or the hub closes. */
  readonly signal: AbortSignal;
}

/** Answers a request. The returned value is the reply (JSON). @since 1.0.0 */
export type RequestHandler<
  Params extends Readonly<Record<string, string>> = Record<string, string>,
> = (context: RequestContext<Params>) => Json | undefined | Promise<Json | undefined>;

/**
 * What a channel pattern allows. Every permission defaults to `false`: a channel nobody wrote a rule for is closed.
 * @since 1.0.0
 */
export interface ChannelRule<
  Params extends Readonly<Record<string, string>> = Record<string, string>,
> {
  readonly subscribe?: Permission<Params>;
  readonly publish?: Permission<Params>;
  readonly presence?: Permission<Params>;
  /** Messages kept for resume (default: the hub's `history` limit; 0 turns resume off for these channels). */
  readonly history?: number;
  /** Request handlers by event name. */
  readonly handlers?: Readonly<Record<string, RequestHandler<Params>>>;
}

type Part =
  | { readonly kind: "literal"; readonly text: string }
  | { readonly kind: "param"; readonly name: string }
  | { readonly kind: "rest"; readonly name: string };

/** A compiled rule. @internal */
export interface CompiledRule {
  readonly pattern: string;
  readonly parts: readonly Part[];
  readonly rule: ChannelRule;
}

const PARAM = /^\{([A-Za-z_][A-Za-z0-9_]*)\}$/;
const REST = /^\{\.\.\.([A-Za-z_][A-Za-z0-9_]*)\}$/;
const LITERAL = /^[A-Za-z0-9_.@/-]+$/;

/**
 * Compiles a channel pattern: `:`-separated segments that are literals, `{name}` (one segment) or a final `{...name}` (one or
 * more segments).
 * @throws TypeError for an invalid pattern.
 */
export function compileRule(pattern: string, rule: ChannelRule): CompiledRule {
  if (typeof pattern !== "string" || pattern.length === 0 || pattern.length > 128)
    throw new TypeError(`the channel pattern "${String(pattern)}" must be 1..128 characters.`);
  const segments = pattern.split(":");
  const seen = new Set<string>();
  const parts = segments.map((segment, at): Part => {
    const rest = REST.exec(segment);
    if (rest !== null) {
      if (at !== segments.length - 1)
        throw new TypeError(
          `the channel pattern "${pattern}": {...name} must be the last segment.`,
        );
      const name = rest[1] as string;
      if (seen.has(name))
        throw new TypeError(`the channel pattern "${pattern}" repeats "${name}".`);
      seen.add(name);
      return { kind: "rest", name };
    }
    const param = PARAM.exec(segment);
    if (param !== null) {
      const name = param[1] as string;
      if (seen.has(name))
        throw new TypeError(`the channel pattern "${pattern}" repeats "${name}".`);
      seen.add(name);
      return { kind: "param", name };
    }
    if (!LITERAL.test(segment) || segment.startsWith("$"))
      throw new TypeError(`the channel pattern "${pattern}" has an invalid segment "${segment}".`);
    return { kind: "literal", text: segment };
  });
  if (rule.history !== undefined && (!Number.isInteger(rule.history) || rule.history < 0))
    throw new RangeError(`the rule for "${pattern}": history must be a non-negative integer.`);
  return { pattern, parts, rule };
}

/** The params a name captures against a rule, or undefined when it does not match. */
export function matchRule(
  compiled: CompiledRule,
  channel: string,
): Record<string, string> | undefined {
  const segments = channel.split(":");
  if (segments.includes("")) return undefined;
  const { parts } = compiled;
  const last = parts[parts.length - 1];
  if (last?.kind === "rest" ? segments.length < parts.length : segments.length !== parts.length)
    return undefined;
  const params: Record<string, string> = {};
  for (let at = 0; at < parts.length; at += 1) {
    const part = parts[at] as Part;
    const segment = segments[at] as string;
    if (part.kind === "rest") {
      params[part.name] = segments.slice(at).join(":");
      return params;
    }
    if (part.kind === "literal") {
      if (segment !== part.text) return undefined;
    } else {
      params[part.name] = segment;
    }
  }
  return params;
}

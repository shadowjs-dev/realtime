import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadProduction, messageOf } from "./support/production-build";

type ProtocolModule = typeof import("../src/protocol");
type LimitsModule = typeof import("../src/server/limits");
type RulesModule = typeof import("../src/server/rules");

const CODE = /^REALTIME_[A-Z]+(?:_\d+)?$/;

describe("production error codes of the protocol and the hub options", () => {
  it("[ERR-CODES-4] every refused client frame explains itself with a REALTIME_ code, and different refusals carry different codes", async () => {
    const protocol = await loadProduction<ProtocolModule>(
      join(import.meta.dirname, "../src/protocol.ts"),
    );
    const frames: Record<string, string> = {
      json: "{",
      object: "[]",
      n: '{"t":"ping","n":-1}',
      channel: '{"t":"sub","n":1,"c":"bad channel"}',
      version: '{"t":"hello","v":0}',
      subNeedsN: '{"t":"sub","c":"room"}',
      after: '{"t":"sub","n":1,"c":"room","after":-1}',
      echo: '{"t":"sub","n":1,"c":"room","echo":1}',
      watch: '{"t":"sub","n":1,"c":"room","watch":1}',
      event: '{"t":"pub","n":1,"c":"room","e":"bad event"}',
      key: '{"t":"pub","n":1,"c":"room","e":"x","k":""}',
      ms: '{"t":"req","n":1,"c":"room","e":"x","ms":-1}',
      type: '{"t":"nope"}',
    };
    const whys = new Map<string, string>();
    for (const [name, text] of Object.entries(frames)) {
      const result = protocol.decodeClientFrame(text, 1024);
      expect(result.ok, name).toBe(false);
      if (!result.ok) whys.set(name, result.why ?? "");
    }
    const tooLarge = protocol.decodeClientFrame("x".repeat(2048), 1024);
    expect(tooLarge.ok).toBe(false);
    if (!tooLarge.ok) whys.set("size", tooLarge.why ?? "");
    for (const [name, why] of whys) expect(why, name).toMatch(CODE);
    expect(new Set(whys.values()).size).toBe(whys.size);
  });

  it("[ERR-CODES-5] every refused limit and channel rule carries a REALTIME_ code, and different refusals carry different codes", async () => {
    const limits = await loadProduction<LimitsModule>(
      join(import.meta.dirname, "../src/server/limits.ts"),
    );
    const rules = await loadProduction<RulesModule>(
      join(import.meta.dirname, "../src/server/rules.ts"),
    );
    const messages = {
      limitName: messageOf(() => limits.resolveLimits({ nope: 1 } as never)),
      limitValue: messageOf(() => limits.resolveLimits({ maxMessageBytes: 0 })),
      limitHistory: messageOf(() => limits.resolveLimits({ history: 1_000_000_000 })),
      limitTtl: messageOf(() =>
        limits.resolveLimits({ presenceTtlMs: 1000, presenceHeartbeatMs: 2000 }),
      ),
      patternLength: messageOf(() => rules.compileRule("", {})),
      patternRest: messageOf(() => rules.compileRule("{...a}:b", {})),
      patternRepeat: messageOf(() => rules.compileRule("{a}:{a}", {})),
      patternSegment: messageOf(() => rules.compileRule("a b", {})),
      ruleHistory: messageOf(() => rules.compileRule("room", { history: -1 })),
    };
    for (const [name, message] of Object.entries(messages)) {
      expect(message, name).toMatch(CODE);
    }
    expect(new Set(Object.values(messages)).size).toBe(Object.keys(messages).length);
  });
});

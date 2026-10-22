import { afterEach, describe, expect, it } from "vitest";
import { cloudflareAdapter } from "../src/cloudflare";
import { adapterConformance } from "../src/testing";
import { FakeNamespace } from "./support/do-host";

// The conformance suite every adapter must pass, against the Durable Object adapter wired to the channel object running on a
// fake runtime (storage double, in-process sockets). The same suite runs against real workerd in `npm run smoke:realtime-cloudflare`.
const spaces = new Map<string, FakeNamespace>();
afterEach(() => {
  for (const space of spaces.values()) space.dispose();
  spaces.clear();
});

adapterConformance({
  runner: { describe, it, expect, afterEach: (body) => afterEach(body) },
  connect: (namespace) => {
    let space = spaces.get(namespace);
    if (space === undefined) {
      space = new FakeNamespace({ autoAlarm: true });
      spaces.set(namespace, space);
    }
    return cloudflareAdapter({ namespace: space, presenceGraceMs: 2000 });
  },
});

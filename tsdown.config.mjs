import { createConfig } from "./build/tsdown-preset.mjs";

// Each side is a subpath of its own: a page pays only for the client, a server only for the hub and the adapter it picks.
export default createConfig({
  packageDir: import.meta.dirname,
  entries: {
    index: "src/index.ts",
    "server/index": "src/server/index.ts",
    "client/index": "src/client/index.ts",
    hooks: "src/hooks.ts",
    redis: "src/redis.ts",
    cloudflare: "src/cloudflare.ts",
    testing: "src/testing.ts",
  },
});

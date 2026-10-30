---
name: shadowjs-realtime
description: "Work with @shadow-js/realtime. Typed channels, presence and resumable delivery over WebSocket or server-sent events. Use when a project imports it, or when asked to add, configure or debug it."
---

# @shadow-js/realtime

Version 1.0.4. Typed channels, presence and resumable delivery over WebSocket or server-sent events.

## Where to read more

- @shadow-js/realtime: https://realtime.shadowjs.net
- Source and issues: https://github.com/shadowjs-dev/realtime
- The package README is the full reference for options and behavior; read it before changing a configuration.

## Install

```sh
npm install @shadow-js/realtime
```

## Peer dependencies

| Package | Range | Required |
| --- | --- | --- |
| `@shadow-js/core` | `^1.0.4` | no |

## Import paths

Import only from these paths. Never from `dist` or `src`: files that are not listed here are private and change without notice.

- `@shadow-js/realtime`
- `@shadow-js/realtime/server`
- `@shadow-js/realtime/client`
- `@shadow-js/realtime/hooks`
- `@shadow-js/realtime/redis`
- `@shadow-js/realtime/cloudflare`
- `@shadow-js/realtime/testing`

## Quick start

```ts
import { createHub } from "@shadow-js/realtime/server";
import { connectTest } from "@shadow-js/realtime/testing";
import { expect, it } from "vitest";

it("delivers a message to a subscriber, in order, with an acknowledgment", async () => {
  const hub = createHub({
    channels: {
      "chat:{room}": { subscribe: true, publish: ({ identity }) => identity.user !== undefined },
    },
  });
  const ann = connectTest(hub, { user: "ann" });
  const bob = connectTest(hub, { user: "bob" });
  bob.send({ t: "sub", n: 1, c: "chat:lobby" });
  await bob.settled();
  ann.send({ t: "pub", c: "chat:lobby", e: "message", d: { text: "hi" }, n: 2 });
  await ann.settled();

  expect(bob.frames.find((frame) => frame.t === "msg")).toMatchObject({ s: 1, d: { text: "hi" } });
  expect(ann.frames.find((frame) => frame.t === "ack")).toEqual({ t: "ack", n: 2, s: 1 });
  await hub.close();
});
```

## Working rules

- Run `npm run check` before proposing a change; it lints, builds, type-checks and tests.
- Keep a public change documented: TSDoc on every exported symbol, and a CHANGELOG entry.
- To change this repository rather than use the package, read AGENTS.md: the layout, the commands, the rules and how a pull request is reviewed.

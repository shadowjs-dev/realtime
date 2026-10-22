<p align="center"><img src="https://raw.githubusercontent.com/shadowjs-dev/realtime/main/.github/banner.png" alt="ShadowJS" width="100%"></p>

# @shadow-js/realtime

[![npm](https://img.shields.io/npm/v/@shadow-js/realtime.svg)](https://www.npmjs.com/package/@shadow-js/realtime)
[![CI](https://github.com/shadowjs-dev/realtime/actions/workflows/ci.yml/badge.svg)](https://github.com/shadowjs-dev/realtime/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](https://github.com/shadowjs-dev/realtime/blob/main/LICENSE)
[![Node.js](https://img.shields.io/badge/node-%3E%3D22.12-brightgreen.svg)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-ready-blue.svg)](https://www.typescriptlang.org/)

Typed channels, presence and resumable delivery over WebSocket or server-sent events.

- **Typed channels.** One schema gives every channel its events, presence state and requests, checked at compile time.
- **Resumable.** Every message has a number. A client that drops and reconnects resumes from its last number: no loss and no duplicates inside the retention window, and a clear signal when the window was exceeded.
- **Closed by default.** A channel nobody wrote a rule for cannot be subscribed to or published to.
- **Scales out.** The in-memory adapter serves one process; the Redis adapter (Redis 6.2+ and Valkey) and the Cloudflare Durable Objects adapter serve many.
- **Runs where Dusk runs.** WebSocket where the host holds sockets, server-sent events plus HTTP posts where it does not (Vercel, Netlify).

## Install

```sh
npm install @shadow-js/realtime
```

## Quick start

A hub is the server. Rules say who may do what; `connectTest` talks to it with no network.

```ts title="chat.test.ts" test
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

In a Dusk app the hub is route exports. `routes/rt.ts` takes WebSockets and server-sent events; `routes/rt/send.ts` exports `POST = hub.post()` for what an SSE client sends:

```ts title="routes/rt.ts" typecheck
import { createHub } from "@shadow-js/realtime/server";

export const hub = createHub({
  channels: {
    "chat:{room}": { subscribe: true, publish: ({ identity }) => identity.user !== undefined },
  },
});

export const WS = hub.websocket();
export const SSE = hub.sse();
```

The browser opens one typed connection:

```ts title="client.ts" typecheck
import { createRealtime } from "@shadow-js/realtime/client";

type Schema = {
  [name: `chat:${string}`]: { events: { message: { text: string } } };
};

const rt = createRealtime<Schema>({ url: "/rt" });
const lobby = rt.channel("chat:lobby");
lobby.on("message", (data, meta) => console.log(meta.seq, data.text));
await lobby.publish("message", { text: "hello" });
```

## API

Root, `@shadow-js/realtime`: the wire protocol and shared types.

- `RealtimeError` and `ErrorCode`: an error with a protocol code.
- `PROTOCOL_VERSION`, `decodeClientFrame`, `decodeServerFrame`, `isChannelName`, `isEventName`: the frame codecs and name validators.

`@shadow-js/realtime/server`:

- `createHub(options)`: the server. `hub.channel(pattern, rule)`, `hub.publish(channel, event, data)`, `hub.revoke(target)`, `hub.stats()`, `hub.close()`; the routes `hub.websocket()`, `hub.sse()`, `hub.post()`.
- `memoryAdapter()`: the single-process adapter (the default). `DEFAULT_HUB_LIMITS`: every limit and its default.

`@shadow-js/realtime/client`:

- `createRealtime(options)`: the client. `rt.channel(name, options)` returns a channel with `on`, `onAny`, `onGap`, `publish`, `request`, `presence` and `close`.

`@shadow-js/realtime/hooks`:

- `useChannel`, `useChannelEvent`, `useChannelPresence`, `useRealtimeStatus`: signal-based hooks that release their channel with the component.

`@shadow-js/realtime/redis`: `redisAdapter`, `fromIoredis`, `fromNodeRedis`.
`@shadow-js/realtime/cloudflare`: `cloudflareAdapter`, `RealtimeChannelObject`.
`@shadow-js/realtime/testing`: `connectTest`, `adapterConformance`.

## Learn more

See the ShadowJS documentation at https://shadowjs.net, and the realtime guide at https://realtime.shadowjs.net.

## Using, changing and contributing

- **Use the package** (a person or a coding agent that builds with it): this README, and [SKILL.md](SKILL.md), the short page an agent reads first.
- **Change the package** (fix a bug, add a feature): [AGENTS.md](AGENTS.md) says how the repository is laid out, how to implement and test a change, and how a pull request is reviewed. It is written for people and coding agents alike.
- **Take part**: [CONTRIBUTING.md](CONTRIBUTING.md) is the short guide; [SECURITY.md](SECURITY.md) is for vulnerabilities.

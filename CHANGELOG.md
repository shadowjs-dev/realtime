# Changelog

## 1.0.3

### Fixed

- Idempotency keys belong to their publisher: the hub stores `key/u:<user>` (a signed-in user), `key/a` (anonymous connections) or `key/s` (`hub.publish`), so another user or the server reusing a key is never a duplicate.
- `maxPresenceMembers` holds under concurrent joins: joins to one channel are applied one at a time per hub.
- A `revoke` issued while a subscribe is awaiting its async rule refuses that subscribe (and counts it).
- After `subbed gap:true` the client resumes from the gap's head, so a gap with nothing left to replay is reported once, not on every reconnect.

## 1.0.2

### Fixed

- Client: adding the same handler to a channel twice no longer leaves a subscription hold that nothing can release.
- Client: a POST that never answers (server-sent events transport) is abandoned after the acknowledgment timeout, so the publishes queued behind it are sent.
- Server: a stream declaration that names its own `t` or `n` is always read as a subscription, never as a publish or request.
- Server: a WebSocket that closes while `authenticate` is still running no longer leaves a connection in the hub.
- Server: presence changes posted over HTTP spend from the same rate limit as publishes, and a POST body is read only up to the size limit.
- Server: a member whose presence entry expired is written back with its last state instead of `null`; a transport whose `send` throws is closed.
- Memory adapter: idempotency keys are capped per channel (65,536), and expired ones are swept when the map doubles.
- Redis adapter: a channel whose keys were lost and whose numbering restarted still delivers its messages (the hub tells subscribers `reset`).
- Client: the event stream parser holds at most 1 MiB for an unfinished line and event; a longer one closes the stream and reconnects. A tab that becomes visible while the client waits out a reconnect backoff reconnects at once.
- Server: `sub` and `unsub` frames spend from a per-connection bucket (the publish rate, a burst that covers all of `maxChannelsPerConnection`), so subscription churn is limited like publishing.
- Server: a frame over `maxMessageBytes` is refused when it arrives instead of after the frames queued before it; it counts as an invalid frame.
- Server: a presence state posted over HTTP also updates the connection's own copy (on this server directly, on another through the presence event), so a write-back after an expiry carries the newest version and state.
- Cloudflare adapter: a listener whose queue overflows is reported (`onError`), and a presence listener is resynced to the real member set (leaves, joins, changes) instead of staying wrong.
- Errors while dropping a connection and listener failures in the memory adapter go to `onError` (memory adapter: new `onError` option, default `console.error`) instead of being swallowed.
- The realtime client takes `reconnect.maxReconnectAttempts` (default unlimited). After that many failed reconnects in a row (the count restarts when a connection stays open `stableMs`) the client is `closed`, pending calls reject, and `onError` receives a `closed` error.

## 1.0.1

### Changed

- Released at 1.0.1 with the other packages, which share one version. No change to this package.

## 1.0.0

### Added

- Typed channels with rules that deny by default, per-channel numbering, resume with replay and gap detection, acknowledged and
  idempotent publishes, request and reply, presence with heartbeats and exactly-once leave, rate limits, slow-consumer protection and
  revocation.
- WebSocket and server-sent-events transports with an HTTP post route, a client with jittered reconnect, an offline queue and
  automatic fallback, and signal-based hooks.
- Adapters: in memory, Redis (6.2+ and Valkey) and Cloudflare Durable Objects, all passing one conformance suite
  (`adapterConformance`), plus `connectTest` and `memoryWebSocket` for tests.

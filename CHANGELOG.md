# Changelog

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

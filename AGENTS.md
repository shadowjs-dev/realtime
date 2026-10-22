# AGENTS.md: working on realtime

This file is for anyone who changes this repository, people and coding agents alike. It says how the repository is laid out, how to implement
and test a change, and how a pull request is reviewed. To **use** the released package, read [README.md](README.md) and [SKILL.md](SKILL.md)
instead; to take part in the project, read [CONTRIBUTING.md](CONTRIBUTING.md).

Typed channels, presence and resumable delivery over WebSocket or server-sent events.

## What is in this repository

| Package | Folder | What it is | Documentation |
| --- | --- | --- | --- |
| `@shadow-js/realtime` | `.` | Typed channels, presence and resumable delivery over WebSocket or server-sent events | https://realtime.shadowjs.net |

The repository is the package: `@shadow-js/realtime`. Source and issues:
https://github.com/shadowjs-dev/realtime. Every package of the project has the same version number.

## Set up

You need Node.js 22.12 or newer and npm (pnpm and Yarn are not used).

The tests also need:

- Ubuntu packages: `redis-server`.

A test whose tool or account is missing **fails** with instructions; it is never skipped.

```sh
npm install
npm run check   # lint, build, type check and every test
```

## Commands

| Command | What it does |
| --- | --- |
| `npm run lint` | Biome: lint and format check (`npx biome check --write <path>` fixes what it can) |
| `npm run build` | builds the package with tsdown |
| `npm run typecheck` | TypeScript 7 in strict mode, tests included |
| `npm test` | unit, DOM and server tests (Vitest) |
| `npx vitest run <path>` | one test file |
| `npm run test:gc` | memory and leak tests |
| `npm run test:perf` | wall-clock budgets; run alone, never in parallel with other work |
| `npm run check` | all of the above; a change is ready when it passes |

## Layout

- `.github/`: workflows, issue and pull request templates, code owners.
- `build/`: the shared tsdown preset every package build uses.
- `scripts/`: build and release scripts; `release.mjs` is run by the workflows.
- `src/`: the source of the package; one folder per concern, `index.ts` re-exports the public API. Folders: `client/`, `cloudflare/`, `redis/`, `server/`.
- `test/`: the tests; one file per behavior, each test named by an invariant id such as `[ROUTER-12]`.
- `types/`: ambient declarations shared with the build.
- `CHANGELOG.md`: what each release changed.
- `biome.json`: lint and format rules.
- `package.json`: the manifest; `version` is changed only by a release.
- `tsconfig.json`: type check of the source.
- `tsdown.config.mjs`: the build.
- `vitest.config.ts`: the test projects (unit, DOM, server, gc, perf).
- `vitest.setup.ts`: test setup: an unasserted `console.warn` or `console.error` fails a test.

## How to implement a change

1. **Find where the behavior lives.** Search `test/` and `src/` for the name of the behavior or its invariant id, then read the source the tests exercise. The invariants below say what must stay true.
2. **Write the failing test first.** A bug fix reproduces the bug; a feature states the behavior it promises. Name the test with an invariant id in square brackets, the way the neighboring tests do, then the behavior in words. The test asserts what the code does and is never skipped, marked as todo or marked as an expected failure.
3. **Make it pass with the smallest change** that keeps the invariants. Match the code around it.
4. **Keep the surface honest.** A public symbol has TSDoc (summary, parameters, return value, an example and `@since`). An error says what happened and how to fix it. User-visible behavior gets a documentation update and a line under `## Unreleased` in CHANGELOG.md.
5. **Run `npm run check`** and read its output; do not report a change as done without it passing.

### Rules every change keeps

- No `any`, `@ts-ignore` or non-null assertion. Use `unknown` and narrow.
- Compare indices, lengths and counts explicitly (`=== undefined`, `=== -1`, `> 0`), never by truthiness.
- No empty `catch`: every catch rethrows, passes the error to the owner's handler, or reports it. Every "set the current X" register is restored in `finally`.
- User callbacks run untracked unless the API says otherwise. No stack parsing, no `fn.name` sniffing, no flags stamped on user functions.
- Traversals whose depth the user controls are iterative. Caches, queues and recursion are bounded, and a test hits the bound.
- Code meant for development only sits behind `__DEV__`. No import-time side effects and no module-level mutable state beyond the registers the core owns (bounded caches and `WeakMap`s excepted).
- No sleeps in tests (flush the scheduler); a `console.warn` or `console.error` that a test does not assert fails the test. Property tests use `fast-check` with a fixed seed.
- Files are kebab-case, tests are `*.test.ts`, internals live in `src/internal/`. American English in code, comments and documentation. Comments explain why, not what.
- The reactive core and the DOM renderer have no runtime dependencies.

## Pull requests

1. **Open an issue first** for anything that adds or changes public API, behavior, supported platforms or dependencies. A small fix needs no issue.
2. **Fork, then branch from `main`** (`fix/<topic>`, `feat/<topic>`, `docs/<topic>`). One concern per pull request.
3. **Commit in small steps** with a message that says what changed and why: a short type prefix (`fix:`, `feat:`, `docs:`, `test:`, `chore:`), then the subject. Commit under your own identity; do not add trailers or notes about tools.
4. **Open the pull request against `main`** and fill in the template: what changes, why, and the checks you ran. The CI workflow (`.github/workflows/ci.yml`) runs lint, build, type check and all tests; it must be green.
5. **Review.** A maintainer (see `.github/CODEOWNERS`) reviews every pull request. Expect questions about the test, the public surface and the error messages. Push fixes to the same branch; the pull request is squash-merged.
6. **Releases are not part of a pull request.** Do not change `version`, `.github/workflows/release.yml` or `scripts/release.mjs`, and do not create tags or publish. A maintainer releases by renaming `## Unreleased` in the changelog to the version and pushing a version that is not yet on npm to `main`; the workflow publishes it with provenance.

Security problems never go in a public issue or pull request: follow [SECURITY.md](SECURITY.md).

## Architecture and invariants

What each package keeps true. A change that breaks one of these is a bug in the change.

The protocol, guarantees and rules are documented in the realtime protocol reference (the site's guides); this file is the map of the code and the list of rules a change must keep. Every
cited id is named by a test of this package.

### Layout

- `src/protocol.ts`, `src/errors.ts`: the wire frames, their strict decoder for client frames (never throws, bounded) and lenient decoder for server frames
  (ignores what it does not know), the name validators, `RealtimeError`. No code here depends on a runtime.
- `src/adapter.ts`, `src/memory.ts`: the adapter interface (storage and fan-out between servers) and the in-memory implementation.
- `src/server/`: `hub.ts` (connections, subscriptions, replay, presence, requests, limits), `rules.ts` (channel patterns and permissions), `limits.ts`
  (bounded defaults, token bucket, sliding counter), `http.ts` (the WebSocket, SSE and POST routes as Dusk files export them), `index.ts` (`createHub`).
- `src/client/`, `src/hooks.ts`: the browser client (one transport, reconnect, queue, resume, channels) and the reactive hooks.
- `src/redis.ts`, `src/cloudflare.ts`: adapters for several servers. `src/testing.ts`: test connections and the adapter conformance suite.

### Invariants

- The hub is transport-agnostic: everything that crosses a connection is a text frame; a transport that throws, blocks or closes affects that connection only
  [RT-G13, RT-G9, RT-G14].
- A channel is a gapless numbered log. Subscribers see strictly increasing seqs without duplicates, resume from their last seq, and are told about a gap or
  a reset instead of silently losing data [RT-G1, RT-G2, RT-G3, RT-G4, RT-G5, RT-G6, RT-G7, RT-G8].
- Nothing is allowed unless a rule says so; a predicate that fails never grants; identity does not change during a connection; revocation is immediate
  [RT-G12, RT-R1, RT-R2, RT-R3, RT-R4].
- Every resource is bounded: frames, channels per connection, subscribers per channel, presence members and state, requests in flight, queued frames,
  buffered bytes, publish rate; abuse is answered, then the connection is closed [RT-R5, RT-R6, RT-R7, RT-R8, RT-G9, RT-G10].
- Presence converges: join, set and leave are ordered by version, a leave is announced once, expiry is detected without any one server staying alive [RT-G11].
- The hub holds no state for a closed connection and no channel without subscribers [RT-SCALE-01].
- Adapters differ only in where state lives: the same conformance suite passes for each [RT-AD-01..10, RT-AD-20..25].
- Inside Dusk the hub is three route exports (`WS`, `SSE`, `POST`) and adds no other global state [RT-DUSK-01..06].
- Client behavior: one transport per client, opened on first use; reconnect with full-jitter backoff reset only after a stable connection; every live
  subscription resumes with the last seq and nothing is delivered twice; publishes queue offline within bounds and resend with their original idempotency
  key; the heartbeat declares a silent link dead; WebSocket falls back to SSE after two failed upgrades; no timer, listener or subscription outlives
  `close()`; hooks add no connection during the server render [RT-C-01..56, RT-R9, RT-R10, RT-R11, RT-R12, RT-NET-01..04, RT-T1, RT-T2, RT-T3, RT-T4, RT-T5, RT-T6, RT-T7].

## Where to look next

- [README.md](README.md): what the package does and every option.
- [SKILL.md](SKILL.md): how to use the released package (install, import paths, a quick start).
- [CONTRIBUTING.md](CONTRIBUTING.md): the short guide to taking part.
- [CHANGELOG.md](CHANGELOG.md): what each release changed.
- [ShadowJS hub](https://github.com/shadowjs-dev/shadow): every repository of the project.

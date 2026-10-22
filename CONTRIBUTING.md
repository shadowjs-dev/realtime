# Contributing to realtime

Thank you for helping. This guide covers how to set up the repository, what a good change looks like, and how it is reviewed.

## Setup

You need Node.js 22.12 or newer and npm.

```sh
npm install
npm run check   # lint, typecheck, build and tests
```

Coding agents and anyone who wants the map of the repository (layout, rules, the way a change is made and reviewed) should read
[AGENTS.md](AGENTS.md). To use the released package instead, read [README.md](README.md) and [SKILL.md](SKILL.md).

## The commands that matter

| Command | What it does |
| --- | --- |
| `npm run lint` | Biome |
| `npm run typecheck` | TypeScript, including the tests |
| `npm run build` | builds the package |
| `npm test` | unit, DOM and server tests |
| `npm run test:gc` | memory and leak tests |
| `npm run test:perf` | wall-clock budgets, run alone |
| `npm run check` | all of the above |

## What a good change looks like

1. **Start with a failing test.** A bug fix reproduces the bug first. A feature starts with the behavior it promises.
2. **Keep it small and focused.** One concern per pull request.
3. **Match the code around it.** TypeScript in strict mode, no `any` without a comment that explains why. Format with Biome
   (`npx biome check --write <file>`).
4. **American English** in code, comments and documentation.
5. **Document it.** Public API needs TSDoc with `@since` and `@example`. User-visible behavior needs documentation and a changelog entry.
6. **Errors are part of the API.** Every thrown or logged problem has a stable code, says what happened and how to fix it.

## Questions and problems

Ask in the Discussions tab of the [ShadowJS hub](https://github.com/shadowjs-dev/shadow). Security problems go through SECURITY.md, never a public issue.

## Conduct and licensing

Everyone taking part follows CODE_OF_CONDUCT.md. By contributing you agree that your contribution is licensed under the MIT license of this
repository.

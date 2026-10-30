#!/usr/bin/env node
// scripts/release.mjs of every public repository. The release workflow calls it on each push to `main`; it publishes what the registry does
// not have yet and does nothing else, so a push that changes no version is a quiet no-op and a run that stopped half way can simply run again.
//
//   node scripts/release.mjs plan    [--registry <url>]                       which packages are not on the registry yet
//   node scripts/release.mjs wait    [--registry <url>] [--timeout-minutes n] waits until the ShadowJS packages this one needs are published
//   node scripts/release.mjs install [npm install arguments]                  `npm install`, tried again when the registry is behind
//   node scripts/release.mjs publish [--registry <url>] [--dry-run]           publishes the missing packages, in dependency order
//
// `plan` writes `pending=true|false` and `version=<x.y.z>` to $GITHUB_OUTPUT when it is set. Set RELEASE_PROVENANCE=false to publish without
// provenance (a registry on this machine has no provider to attest the build). A prerelease is published under the tag `next`, and a
// version below the registry's `latest` under `release-<major>`, so neither takes `latest` from the newest release. No dependencies: it
// runs before `npm install`.
import { spawnSync } from "node:child_process";
import { appendFileSync, existsSync, readdirSync, readFileSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_REGISTRY = "https://registry.npmjs.org";
/** Scopes whose packages are published by the other repositories of the project. */
const SIBLING = /^(?:@shadow-js\/|create-shadow-app$)/;
/** `owner/name` of a GitHub address in any of its forms (https, git+https, ssh), without `.git`. */
const GITHUB_REPOSITORY = /github\.com[/:]([^/]+\/[^/]+?)(?:\.git)?\/?$/i;

/**
 * The packages of a repository: the root when it is one, else the workspace members. Private packages are left out.
 * @param {string} root The repository folder.
 * @returns {{ name: string, version: string, dir: string, manifest: Record<string, unknown> }[]} In the order of the workspaces list.
 */
export function readPackages(root) {
  const read = (dir) => {
    const manifest = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
    return { name: manifest.name, version: manifest.version, dir, manifest };
  };
  const top = read(root);
  const patterns = top.manifest.workspaces;
  if (!Array.isArray(patterns)) return top.manifest.private === true ? [] : [top];
  const dirs = [];
  for (const pattern of patterns) {
    if (pattern.endsWith("/*")) {
      const parent = join(root, pattern.slice(0, -2));
      if (!existsSync(parent)) continue;
      for (const entry of readdirSync(parent).sort()) {
        if (existsSync(join(parent, entry, "package.json"))) dirs.push(join(parent, entry));
      }
    } else dirs.push(join(root, pattern));
  }
  return dirs.map(read).filter((entry) => entry.manifest.private !== true);
}

/**
 * Puts packages after the packages of the same repository they depend on (stable otherwise).
 * @param {{ name: string, manifest: Record<string, unknown> }[]} packages The packages.
 * @returns {typeof packages} The same packages, dependencies first.
 */
export function inDependencyOrder(packages) {
  const names = new Set(packages.map((entry) => entry.name));
  const needs = (entry) =>
    ["dependencies", "peerDependencies", "optionalDependencies"]
      .flatMap((field) => Object.keys(entry.manifest[field] ?? {}))
      .filter((name) => names.has(name) && name !== entry.name);
  const done = new Set();
  const ordered = [];
  const pending = [...packages];
  while (pending.length > 0) {
    const index = pending.findIndex((entry) => needs(entry).every((name) => done.has(name)));
    if (index === -1)
      throw new Error(`a dependency cycle among ${pending.map((p) => p.name).join(", ")}`);
    const [next] = pending.splice(index, 1);
    done.add(next.name);
    ordered.push(next);
  }
  return ordered;
}

const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

/**
 * Reads a semantic version (semver.org, section 2).
 * @param {unknown} version The text.
 * @returns {{ core: number[], pre: string[] | undefined } | undefined} Its parts, or undefined when it is not one. Build metadata is dropped.
 */
export function parseVersion(version) {
  const match = typeof version === "string" ? SEMVER.exec(version) : null;
  if (match === null) return undefined;
  return {
    core: [Number(match[1]), Number(match[2]), Number(match[3])],
    pre: match[4]?.split("."),
  };
}

/**
 * Orders two semantic versions by precedence (semver.org, section 11), not as text: `1.10.0` is after `1.9.0`, and `1.0.0-rc.1` is before `1.0.0`.
 * @param {string} a A version.
 * @param {string} b Another version.
 * @returns {-1 | 0 | 1} Negative when `a` comes first.
 * @throws {Error} When either is not a semantic version.
 */
export function compareVersions(a, b) {
  const x = parseVersion(a);
  const y = parseVersion(b);
  if (x === undefined || y === undefined)
    throw new Error(`"${x === undefined ? a : b}" is not a semantic version`);
  for (let index = 0; index < 3; index += 1) {
    if (x.core[index] !== y.core[index]) return x.core[index] < y.core[index] ? -1 : 1;
  }
  if (x.pre === undefined || y.pre === undefined) {
    if (x.pre === y.pre) return 0;
    return x.pre === undefined ? 1 : -1;
  }
  for (let index = 0; index < Math.min(x.pre.length, y.pre.length); index += 1) {
    const left = x.pre[index];
    const right = y.pre[index];
    if (left === right) continue;
    const leftNumeric = /^\d+$/.test(left);
    const rightNumeric = /^\d+$/.test(right);
    if (leftNumeric && rightNumeric) return Number(left) < Number(right) ? -1 : 1;
    // A numeric identifier is lower than an alphanumeric one.
    if (leftNumeric !== rightNumeric) return leftNumeric ? -1 : 1;
    return left < right ? -1 : 1;
  }
  if (x.pre.length === y.pre.length) return 0;
  return x.pre.length < y.pre.length ? -1 : 1;
}

/**
 * The dist-tag a version must be published under, if not `latest`. A prerelease must not become `latest` (the first install would get it),
 * and neither must a version lower than the current `latest` (a fix for an older line, or a version published late): npm moves `latest` to
 * whatever was published last unless told otherwise.
 * @param {string} version The version about to be published.
 * @param {string | undefined} latest The registry's current `latest`, when it has one.
 * @returns {string | undefined} The tag, or undefined for the default one.
 * @throws {Error} When `version` is not a semantic version.
 */
export function distTagFor(version, latest) {
  const parsed = parseVersion(version);
  if (parsed === undefined) throw new Error(`"${version}" is not a semantic version`);
  if (parsed.pre !== undefined) return "next";
  // A `latest` that is a prerelease or not a version at all is no reason to hold a stable version back.
  const current = parseVersion(latest);
  if (current !== undefined && current.pre === undefined && compareVersions(version, latest) < 0)
    return `release-${parsed.core[0]}`;
  return undefined;
}

const defaultSleep = (ms) => new Promise((done) => setTimeout(done, ms));

/**
 * @typedef {object} RegistryOptions
 * @property {(ms: number) => Promise<void>} [sleep] Waits between attempts (replaced in tests).
 * @property {number} [attempts] How many times to ask before giving up on a 5xx, a 429 or a connection failure.
 * @property {number} [delayMs] The wait before the second attempt; it doubles.
 */

/**
 * The registry's document of a package. A 5xx answer, a 429 and a failed connection are asked again a few times (the registry has
 * short outages, and one must not stop a release); a 404 is "no such package"; any other answer is an error.
 * @param {string} registry The registry URL.
 * @param {string} name The package name.
 * @param {RegistryOptions} [options] Replaced in tests.
 * @returns {Promise<{ versions?: Record<string, unknown>, "dist-tags"?: Record<string, string> } | undefined>} The document, or undefined for a package the registry does not have.
 * @throws {Error} When the registry cannot be asked.
 */
export async function registryDocument(registry, name, options = {}) {
  const { sleep = defaultSleep, attempts = 4, delayMs = 1000 } = options;
  let delay = delayMs;
  for (let attempt = 1; ; attempt += 1) {
    let response;
    try {
      response = await fetch(`${registry}/${name.replace("/", "%2f")}`, {
        headers: { accept: "application/vnd.npm.install-v1+json" },
        // A registry that accepts the connection and never answers must not hold the job until its time limit.
        signal: AbortSignal.timeout(30_000),
      });
    } catch (error) {
      if (attempt >= attempts)
        throw new Error(
          `${registry} could not be reached for ${name}: ${error instanceof Error ? error.message : String(error)}`,
          { cause: error },
        );
      console.log(
        `${registry} could not be reached for ${name} (${error instanceof Error ? error.message : String(error)}); asking again`,
      );
    }
    if (response !== undefined) {
      if (response.status === 404) return undefined;
      if (response.ok) return response.json();
      const transient = response.status >= 500 || response.status === 429;
      if (!transient || attempt >= attempts)
        throw new Error(`${registry} answered ${response.status} for ${name}`);
    }
    await sleep(delay);
    delay = Math.min(delay * 2, 30_000);
  }
}

/**
 * Whether a version is on the registry. A missing package is "no"; anything else that goes wrong is an error, never a "no": a network
 * failure must not look like an unpublished package, or the run would try to publish a version that exists.
 * @param {string} registry The registry URL.
 * @param {string} name The package name.
 * @param {string} version The version.
 * @param {RegistryOptions} [options] Replaced in tests.
 * @returns {Promise<boolean>} True when that version is published.
 * @throws {Error} When the registry cannot be asked.
 */
export async function isPublished(registry, name, version, options = {}) {
  const document = await registryDocument(registry, name, options);
  return document !== undefined && Object.hasOwn(document.versions ?? {}, version);
}

/**
 * The packages of the repository whose version the registry does not have.
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @param {RegistryOptions} [options] Replaced in tests.
 * @returns {Promise<ReturnType<typeof readPackages>>} In publication order.
 */
export async function missingPackages(root, registry, options = {}) {
  const missing = [];
  for (const entry of inDependencyOrder(readPackages(root))) {
    if (!(await isPublished(registry, entry.name, entry.version, options))) missing.push(entry);
  }
  return missing;
}

/**
 * Why the repository cannot be published as it is; empty when it can. Provenance names the repository the build ran in, and npm refuses a
 * package whose `repository.url` is another one, so that mismatch is found here and not after the first publish.
 * @param {ReturnType<typeof readPackages>} packages The packages to publish.
 * @param {string | undefined} repository `owner/name` of the repository the workflow runs in.
 * @returns {string[]} The problems.
 */
export function problemsOf(packages, repository) {
  const problems = [];
  for (const { name, version, manifest } of packages) {
    if (parseVersion(version) === undefined)
      problems.push(`${name}: version ${JSON.stringify(version)} is not a semantic version`);
    const url =
      typeof manifest.repository === "object" ? manifest.repository?.url : manifest.repository;
    if (typeof url !== "string") problems.push(`${name}: package.json has no repository.url`);
    else if (
      repository !== undefined &&
      // The whole owner/name: a prefix match would accept `org/router-extra` for `org/router`.
      GITHUB_REPOSITORY.exec(url)?.[1]?.toLowerCase() !== repository.toLowerCase()
    )
      problems.push(`${name}: repository.url is ${url}, but the workflow runs in ${repository}`);
  }
  return problems;
}

/**
 * The ShadowJS packages from other repositories that this one needs to install and build.
 * @param {ReturnType<typeof readPackages>} packages The packages of the repository.
 * @returns {{ name: string, range: string }[]} Ranges that name a version on the registry.
 */
export function siblingDependencies(packages) {
  const own = new Set(packages.map((entry) => entry.name));
  const needs = new Map();
  for (const { manifest } of packages) {
    const optionalPeers = manifest.peerDependenciesMeta ?? {};
    for (const field of ["dependencies", "peerDependencies", "devDependencies"]) {
      for (const [name, range] of Object.entries(manifest[field] ?? {})) {
        if (!SIBLING.test(name) || own.has(name)) continue;
        if (field === "peerDependencies" && optionalPeers[name]?.optional === true) continue;
        if (/^(?:file|link|workspace|npm):/.test(range)) continue;
        needs.set(`${name}@${range}`, { name, range });
      }
    }
  }
  return [...needs.values()];
}

/**
 * How to start npm without a shell. On Windows `npm.cmd` can only be started through cmd.exe, which reads `>=1.0.0` as a redirect
 * and `^1.0.0` as an escape, so npm runs as a Node script there (the one next to the Node binary, or the one `npm run` names).
 * @param {NodeJS.Platform} [platform] The platform (tests pass another).
 * @param {NodeJS.ProcessEnv} [env] The environment.
 * @param {string} [execPath] The Node binary.
 * @param {(file: string) => boolean} [exists] Whether a file exists.
 * @param {typeof import("node:path")} [path] The path rules of the platform.
 * @returns {{ command: string, args: string[] }} The command and the arguments that come before npm's own.
 * @throws {Error} On Windows, when no npm CLI script is found.
 */
export function npmInvocation(
  platform = process.platform,
  env = process.env,
  execPath = process.execPath,
  exists = existsSync,
  path = { join, dirname },
) {
  if (platform !== "win32") return { command: "npm", args: [] };
  const fromEnv = env.npm_execpath;
  if (fromEnv !== undefined && /\.[cm]?js$/.test(fromEnv) && exists(fromEnv))
    return { command: execPath, args: [fromEnv] };
  const beside = path.join(path.dirname(execPath), "node_modules", "npm", "bin", "npm-cli.js");
  if (exists(beside)) return { command: execPath, args: [beside] };
  throw new Error(`cannot find the npm CLI next to ${execPath}; run this through \`npm run\``);
}

/** Runs npm without a shell. */
function npm(args, options = {}) {
  const { command, args: prefix } = npmInvocation();
  return spawnSync(command, [...prefix, ...args], { encoding: "utf8", shell: false, ...options });
}

/**
 * Whether the registry has a version that satisfies a range (npm does the matching).
 * @param {string} registry The registry URL.
 * @param {string} name The package.
 * @param {string} range The range.
 * @returns {{ found: boolean, detail: string }} Whether it exists, and what npm said when it does not.
 */
function satisfied(registry, name, range) {
  // A timeout: npm waits for a registry that accepted the connection and never answers, and the job would sit there until its time limit.
  const view = npm(["view", `${name}@${range}`, "version", "--registry", registry], {
    timeout: 120_000,
  });
  if (view.status === 0 && view.stdout.trim() !== "") return { found: true, detail: "" };
  const said = (view.stderr || view.stdout || view.error?.message || "").trim();
  return { found: false, detail: said.split("\n")[0] ?? "" };
}

/**
 * Waits until every ShadowJS package this repository depends on can be installed. A repository pushed together with the packages it
 * needs would otherwise fail its first run; here it waits for them (they are published by the other repositories' own runs).
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @param {number} timeoutMinutes How long to wait before giving up.
 * @param {(ms: number) => Promise<void>} [sleep] Replaced in tests.
 */
export async function waitForDependencies(root, registry, timeoutMinutes, sleep = defaultSleep) {
  const needs = siblingDependencies(readPackages(root));
  const started = Date.now();
  let waiting = needs;
  while (waiting.length > 0) {
    const results = waiting.map((need) => ({
      need,
      ...satisfied(registry, need.name, need.range),
    }));
    waiting = results.filter((result) => !result.found).map((result) => result.need);
    if (waiting.length === 0) break;
    const detail = results.find((result) => !result.found)?.detail ?? "";
    const label = waiting.map((need) => `${need.name}@${need.range}`).join(", ");
    if (Date.now() - started > timeoutMinutes * 60_000)
      throw new Error(
        `still not on ${registry} after ${timeoutMinutes} minutes: ${label} (${detail}). Publish the repository that owns them first.`,
      );
    console.log(`waiting for ${label}`);
    await sleep(30_000);
  }
  console.log(
    needs.length === 0
      ? "no other ShadowJS package is needed"
      : "every ShadowJS package needed is published",
  );
}

/** What npm says when the version is already held (published, or staged and not listed yet). */
const ALREADY_PUBLISHED = /cannot publish over (?:the )?previously (?:published|staged)/i;
/**
 * What npm says for a failure that another attempt can cure: a conflict while the registry still processes the package (E409), a
 * rate limit (E429), a server error (E5xx) or a broken connection. Anything else (E401, E403, E404, a bad manifest) fails at once.
 */
const TRANSIENT_FAILURE =
  /\bE(?:409|429|5\d\d)\b|ECONNRESET|ETIMEDOUT|EAI_AGAIN|ECONNREFUSED|socket hang up/i;

/**
 * Publishes the packages the registry does not have, then checks that each one is there.
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @param {object} options
 * @param {boolean} options.dryRun Prints and publishes nothing.
 * @param {(ms: number) => Promise<void>} [options.sleep] Waits between polls (replaced in tests).
 * @param {(name: string, version: string) => Promise<boolean>} [options.listed] Whether the registry lists a version (replaced in tests).
 * @param {number} [options.listingMinutes] How long to wait for a published version to be listed.
 * @param {number} [options.publishAttempts] How many times `npm publish` is run for one package when it fails in a way another attempt can cure.
 * @returns {Promise<string[]>} `name@version` of what was published (or would be).
 * @throws {Error} When a publish fails or the version does not appear on the registry afterward.
 */
export async function publishMissing(
  root,
  registry,
  { dryRun, sleep = defaultSleep, listed = undefined, listingMinutes = 20, publishAttempts = 3 },
) {
  const isListed = listed ?? ((name, version) => isPublished(registry, name, version, { sleep }));
  const missing = await missingPackages(root, registry, { sleep });
  const problems = problemsOf(missing, process.env.GITHUB_REPOSITORY);
  if (problems.length > 0) throw new Error(problems.join("\n"));
  const onNpm = new URL(registry).hostname === "registry.npmjs.org";
  if (!dryRun && onNpm && !process.env.NODE_AUTH_TOKEN)
    throw new Error(
      "NODE_AUTH_TOKEN is empty: add the NPM_TOKEN secret to the repository (an npm automation or granular token that can publish the package and skips 2FA)",
    );
  const published = [];
  for (const { name, version, dir } of missing) {
    // npm moves `latest` to whatever it was given last, so a prerelease, or a version below the current `latest`, gets another tag.
    const latest = (await registryDocument(registry, name, { sleep }))?.["dist-tags"]?.latest;
    const distTag = distTagFor(version, latest);
    const args = [
      "publish",
      dir,
      "--access",
      "public",
      ...(distTag === undefined ? [] : ["--tag", distTag]),
      "--registry",
      registry,
    ];
    if (process.env.RELEASE_PROVENANCE === "false") args.push("--provenance=false");
    else args.push("--provenance");
    if (dryRun) args.push("--dry-run");
    console.log(
      `${dryRun ? "would publish" : "publishing"} ${name}@${version}${distTag === undefined ? "" : ` under the tag ${distTag} (latest is ${latest ?? "none"})`}`,
    );
    let delay = 5_000;
    for (let attempt = 1; ; attempt += 1) {
      const result = npm(args, { stdio: ["inherit", "inherit", "pipe"] });
      const said = result.stderr ?? "";
      process.stderr.write(said);
      if (result.status === 0) break;
      // The first version of a package is staged and listed minutes later, so a run that publishes it, or a run that starts again after
      // one that failed, can be told the version is there although the registry does not list it yet: that is a publish already done.
      if (ALREADY_PUBLISHED.test(said)) {
        console.log(`${name}@${version} is already published; waiting for the registry to list it`);
        break;
      }
      if (attempt >= publishAttempts || !TRANSIENT_FAILURE.test(said))
        throw new Error(`npm publish failed for ${name}@${version}`);
      // A lost answer or a 5xx can come after the registry stored the version: look before publishing again.
      const stored = await isListed(name, version).catch((error) => {
        console.log(`could not ask whether ${name}@${version} is listed: ${error.message}`);
        return false;
      });
      if (stored) break;
      console.log(
        `npm publish failed for ${name}@${version} (attempt ${attempt} of ${publishAttempts}); trying again in ${delay / 1000} s`,
      );
      await sleep(delay);
      delay *= 2;
    }
    published.push(`${name}@${version}`);
    if (dryRun) continue;
    // npm can take several minutes to list the first version of a package it has not seen before (it scans it first); the next package of
    // this repository may need this one to install, and the tag and the release are made only when the version is listed.
    const pollMs = 10_000;
    let visible = false;
    for (let attempt = 0; attempt < (listingMinutes * 60_000) / pollMs && !visible; attempt += 1) {
      visible = await isListed(name, version);
      if (!visible) await sleep(pollMs);
    }
    if (!visible)
      throw new Error(
        `${name}@${version} was published but is not listed by ${registry} after ${listingMinutes} minutes`,
      );
  }
  console.log(
    published.length === 0
      ? "nothing to publish: every version is on the registry"
      : `done: ${published.join(", ")}`,
  );
  return published;
}

/**
 * Runs `npm install` and tries again when it fails. A package that was published a moment ago can be missing from the registry's cached
 * answers for a few minutes (npm caches the list of versions at the edge), and an install that starts right after the wait would fail
 * with "no versions available" although the package exists.
 * @param {string} root The repository folder.
 * @param {string[]} extra More arguments for `npm install`.
 * @param {object} [options] Replaced in tests.
 * @param {(root: string, args: string[]) => number} [options.run] Runs the install and returns its exit status.
 * @param {(ms: number) => Promise<void>} [options.sleep] Waits between attempts.
 * @param {number} [options.attempts] How many times to try.
 * @param {number} [options.delayMs] The wait before the second attempt; it doubles up to five minutes.
 * @returns {Promise<number>} The attempt that succeeded (1 is the first).
 * @throws {Error} When every attempt failed.
 */
export async function installWithRetry(root, extra, options = {}) {
  const {
    run = (cwd, args) => npm(args, { cwd, stdio: "inherit" }).status ?? 1,
    sleep = defaultSleep,
    attempts = 6,
    delayMs = 30_000,
  } = options;
  let delay = delayMs;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    // --prefer-online: the cache is asked again instead of trusted, so a stale answer is not reused.
    if (run(root, ["install", "--prefer-online", ...extra]) === 0) return attempt;
    if (attempt < attempts) {
      console.log(
        `npm install failed (attempt ${attempt} of ${attempts}); trying again in ${Math.round(delay / 1000)} s`,
      );
      await sleep(delay);
      delay = Math.min(delay * 2, 300_000);
    }
  }
  throw new Error(`npm install failed ${attempts} times`);
}

/**
 * What a push would publish.
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @param {RegistryOptions} [options] Replaced in tests.
 * @returns {Promise<{ lines: string[], pending: boolean, version: string }>} One line per package, whether any is missing, and the version of the first package.
 * @throws {Error} When a package has no semantic version: the version is written to the workflow's outputs and becomes a tag name.
 */
export async function plan(root, registry, options = {}) {
  const all = readPackages(root);
  const invalid = all.filter((entry) => parseVersion(entry.version) === undefined);
  if (invalid.length > 0)
    throw new Error(
      invalid
        .map(
          (entry) =>
            `${entry.name}: version ${JSON.stringify(entry.version)} is not a semantic version`,
        )
        .join("\n"),
    );
  const missing = new Set(
    (await missingPackages(root, registry, options)).map((entry) => entry.name),
  );
  return {
    lines: all.map(
      (entry) =>
        `${missing.has(entry.name) ? "to publish" : "on the registry"}: ${entry.name}@${entry.version}`,
    ),
    pending: missing.size > 0,
    version: all[0]?.version ?? "",
  };
}

/**
 * Runs the command line.
 * @param {string[]} argv The arguments after the script name.
 * @param {string} root The repository folder.
 */
export async function main(argv, root) {
  const [command, ...rest] = argv;
  const option = (flag, fallback) => {
    if (!rest.includes(flag)) return fallback;
    const value = rest[rest.indexOf(flag) + 1];
    if (value === undefined || value.startsWith("--")) throw new Error(`${flag} needs a value`);
    return value;
  };
  const registry = option(
    "--registry",
    process.env.RELEASE_REGISTRY ?? process.env.npm_config_registry ?? DEFAULT_REGISTRY,
  ).replace(/\/+$/, "");
  if (command === "plan") {
    const result = await plan(root, registry);
    for (const line of result.lines) console.log(line);
    if (process.env.GITHUB_OUTPUT !== undefined)
      appendFileSync(
        process.env.GITHUB_OUTPUT,
        `pending=${result.pending}\nversion=${result.version}\n`,
      );
  } else if (command === "wait") {
    const minutes = Number(option("--timeout-minutes", "45"));
    // NaN would make `elapsed > NaN` false for ever: the wait would never give up.
    if (!Number.isFinite(minutes) || minutes <= 0)
      throw new Error("--timeout-minutes needs a positive number");
    await waitForDependencies(root, registry, minutes);
  } else if (command === "install") {
    await installWithRetry(root, rest);
  } else if (command === "publish") {
    await publishMissing(root, registry, { dryRun: rest.includes("--dry-run") });
  } else {
    throw new Error(
      "usage: node scripts/release.mjs plan | wait | install | publish [--registry <url>] [--dry-run] [--timeout-minutes <n>]",
    );
  }
}

/**
 * Whether this file is the script node was started with. The paths are compared after resolving links: a script started through a
 * symbolic link (or a path that names it another way) has an `import.meta.url` that is the real file and an `argv[1]` that is not, and
 * comparing those as text would end the run without doing anything and without an error, which for a release looks like "nothing to
 * publish". It stands here and not in a shared module because a public repository has only this file.
 * @param {string} moduleUrl This module's `import.meta.url`.
 * @param {string | undefined} entry The script path node was given (`process.argv[1]`).
 * @param {(file: string) => string} [realpath] Resolves links (replaced in tests).
 * @returns {boolean} True when `moduleUrl` is the entry script.
 */
export function isEntryScript(moduleUrl, entry = process.argv[1], realpath = realpathSync.native) {
  if (entry === undefined) return false;
  const real = (file) => {
    try {
      return realpath(file);
    } catch (error) {
      // A path that does not exist (`node -`, a script removed after it started) cannot be this file.
      if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT")
        return undefined;
      throw error;
    }
  };
  const own = real(fileURLToPath(moduleUrl));
  return own !== undefined && own === real(entry);
}

if (isEntryScript(import.meta.url)) {
  main(process.argv.slice(2), resolve(process.cwd())).catch((error) => {
    console.error(`✖ ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  });
}

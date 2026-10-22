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
// provenance (a registry on this machine has no provider to attest the build). No dependencies: it runs before `npm install`.
import { spawnSync } from "node:child_process";
import { appendFileSync, existsSync, readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const DEFAULT_REGISTRY = "https://registry.npmjs.org";
/** Scopes whose packages are published by the other repositories of the project. */
const SIBLING = /^(?:@shadow-js\/|create-shadow-app$)/;

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

/**
 * Whether a version is on the registry. A missing package is "no"; anything else that goes wrong is an error, never a "no": a network
 * failure must not look like an unpublished package, or the run would try to publish a version that exists.
 * @param {string} registry The registry URL.
 * @param {string} name The package name.
 * @param {string} version The version.
 * @returns {Promise<boolean>} True when that version is published.
 * @throws {Error} When the registry cannot be asked.
 */
export async function isPublished(registry, name, version) {
  const response = await fetch(`${registry}/${name.replace("/", "%2f")}`, {
    headers: { accept: "application/vnd.npm.install-v1+json" },
  });
  if (response.status === 404) return false;
  if (!response.ok) throw new Error(`${registry} answered ${response.status} for ${name}`);
  const document = await response.json();
  return Object.hasOwn(document.versions ?? {}, version);
}

/**
 * The packages of the repository whose version the registry does not have.
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @returns {Promise<ReturnType<typeof readPackages>>} In publication order.
 */
export async function missingPackages(root, registry) {
  const missing = [];
  for (const entry of inDependencyOrder(readPackages(root))) {
    if (!(await isPublished(registry, entry.name, entry.version))) missing.push(entry);
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
  for (const { name, manifest } of packages) {
    const url =
      typeof manifest.repository === "object" ? manifest.repository?.url : manifest.repository;
    if (typeof url !== "string") problems.push(`${name}: package.json has no repository.url`);
    else if (
      repository !== undefined &&
      !url.toLowerCase().includes(`github.com/${repository.toLowerCase()}`)
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

/** Runs npm without a shell on POSIX; Windows needs one for the `.cmd` shim. */
function npm(args, options = {}) {
  return spawnSync(process.platform === "win32" ? "npm.cmd" : "npm", args, {
    encoding: "utf8",
    shell: process.platform === "win32",
    ...options,
  });
}

/**
 * Whether the registry has a version that satisfies a range (npm does the matching).
 * @param {string} registry The registry URL.
 * @param {string} name The package.
 * @param {string} range The range.
 * @returns {{ found: boolean, detail: string }} Whether it exists, and what npm said when it does not.
 */
function satisfied(registry, name, range) {
  const view = npm(["view", `${name}@${range}`, "version", "--registry", registry]);
  if (view.status === 0 && view.stdout.trim() !== "") return { found: true, detail: "" };
  return { found: false, detail: (view.stderr || view.stdout).trim().split("\n")[0] ?? "" };
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

const defaultSleep = (ms) => new Promise((done) => setTimeout(done, ms));

/**
 * Publishes the packages the registry does not have, then checks that each one is there.
 * @param {string} root The repository folder.
 * @param {string} registry The registry URL.
 * @param {object} options
 * @param {boolean} options.dryRun Prints and publishes nothing.
 * @param {(ms: number) => Promise<void>} [options.sleep] Waits between polls (replaced in tests).
 * @param {(name: string, version: string) => Promise<boolean>} [options.listed] Whether the registry lists a version (replaced in tests).
 * @param {number} [options.listingMinutes] How long to wait for a published version to be listed.
 * @returns {Promise<string[]>} `name@version` of what was published (or would be).
 * @throws {Error} When a publish fails or the version does not appear on the registry afterward.
 */
export async function publishMissing(
  root,
  registry,
  { dryRun, sleep = defaultSleep, listed = undefined, listingMinutes = 20 },
) {
  const isListed = listed ?? ((name, version) => isPublished(registry, name, version));
  const missing = await missingPackages(root, registry);
  const problems = problemsOf(missing, process.env.GITHUB_REPOSITORY);
  if (problems.length > 0) throw new Error(problems.join("\n"));
  const onNpm = new URL(registry).hostname === "registry.npmjs.org";
  if (!dryRun && onNpm && !process.env.NODE_AUTH_TOKEN)
    throw new Error(
      "NODE_AUTH_TOKEN is empty: add the NPM_TOKEN secret to the repository (an npm automation or granular token that can publish the package and skips 2FA)",
    );
  const published = [];
  for (const { name, version, dir } of missing) {
    // A prerelease must not become `latest`: the first install of the package would get it.
    const tag = version.includes("-") ? ["--tag", "next"] : [];
    const args = ["publish", dir, "--access", "public", ...tag, "--registry", registry];
    if (process.env.RELEASE_PROVENANCE === "false") args.push("--provenance=false");
    else args.push("--provenance");
    if (dryRun) args.push("--dry-run");
    console.log(`${dryRun ? "would publish" : "publishing"} ${name}@${version}`);
    const result = npm(args, { stdio: ["inherit", "inherit", "pipe"] });
    process.stderr.write(result.stderr ?? "");
    // The first version of a package is staged and listed minutes later, so a run that publishes it, or a run that starts again after
    // one that failed, can be told the version is there although the registry does not list it yet: that is a publish already done.
    const already = /cannot publish over (?:the )?previously (?:published|staged)/i.test(
      result.stderr ?? "",
    );
    if (result.status !== 0 && !already)
      throw new Error(`npm publish failed for ${name}@${version}`);
    if (already)
      console.log(`${name}@${version} is already published; waiting for the registry to list it`);
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
 * @returns {Promise<{ lines: string[], pending: boolean, version: string }>} One line per package, whether any is missing, and the version of the first package.
 */
export async function plan(root, registry) {
  const missing = new Set((await missingPackages(root, registry)).map((entry) => entry.name));
  const all = readPackages(root);
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
  const option = (flag, fallback) =>
    rest.includes(flag) ? rest[rest.indexOf(flag) + 1] : fallback;
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
    await waitForDependencies(root, registry, Number(option("--timeout-minutes", "45")));
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

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2), resolve(process.cwd())).catch((error) => {
    console.error(`✖ ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  });
}

import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type Endpoint, RespClient } from "./resp";

/** Where the tests find a server binary when it is not on PATH. */
const CANDIDATES = ["/opt/homebrew/bin/valkey-server", "/usr/local/bin/valkey-server"];
const NAMES = ["valkey-server", "redis-server"];

/** The server binary, or undefined. Looked up once. */
function findBinary(): string | undefined {
  const given = process.env["REDIS_SERVER_BIN"];
  for (const candidate of [...(given === undefined ? [] : [given]), ...CANDIDATES, ...NAMES]) {
    if (candidate.includes("/") && !existsSync(candidate)) continue;
    const probe = spawnSync(candidate, ["--version"], { stdio: "ignore" });
    if (probe.status === 0) return candidate;
  }
  return undefined;
}

/** The binary, or undefined; and whether an external server is configured. */
export const BINARY: string | undefined = findBinary();
export const EXTERNAL_URL: string | undefined = process.env["REDIS_URL"] || undefined;
/** True when these tests can run: a server to start or one to use. */
export const AVAILABLE: boolean = EXTERNAL_URL !== undefined || BINARY !== undefined;
/** Why they fail when they cannot run. */
export const NO_SERVER =
  "no valkey-server or redis-server binary was found and REDIS_URL is not set: install one (for example `brew install valkey` or `apt install redis-server`) or set REDIS_URL";

/** A running server the tests talk to. */
export interface TestServer {
  readonly endpoint: Endpoint;
  /** True when it is the server named by REDIS_URL (never stopped by the tests). */
  readonly external: boolean;
  stop(): Promise<void>;
}

/** Ports {@link freePort} already returned in this process (a few hundred at most per run, so the set needs no cap). */
const handedOut = new Set<number>();

/** Whether a server can listen on `port` on the loopback address right now. */
function canListen(port: number): Promise<boolean> {
  return new Promise((resolveFree) => {
    const server = createServer();
    server.once("error", () => resolveFree(false));
    server.listen(port, "127.0.0.1", () => server.close(() => resolveFree(true)));
  });
}

/** A TCP port that was free a moment ago on 127.0.0.1, picked at random below the ephemeral range. */
async function freePort(): Promise<number> {
  // Port 0 hands out the next ephemeral port, the same pool every outbound socket draws from, so a parallel suite
  // (or a fetch) often takes it between this probe and the server's listen. A random port below the ephemeral
  // range, never handed out twice by this process, makes that collision rare instead of routine.
  for (let attempt = 0; attempt < 64; attempt += 1) {
    const port = 20_000 + Math.floor(Math.random() * 25_000);
    if (handedOut.has(port)) continue;
    if (await canListen(port)) {
      handedOut.add(port);
      return port;
    }
  }
  throw new Error("freePort: no free port found in 64 attempts between 20000 and 45000");
}

async function ready(endpoint: Endpoint, child: ChildProcess | undefined): Promise<void> {
  const stop = Date.now() + 10_000;
  for (;;) {
    if (child?.exitCode !== null && child?.exitCode !== undefined)
      throw new Error(`the test server exited with code ${child.exitCode}.`);
    const client = new RespClient(endpoint);
    try {
      if ((await client.sendCommand(["PING"])) === "PONG") return;
    } catch {
      // Not up yet.
    } finally {
      await client.close();
    }
    if (Date.now() > stop) throw new Error("the test server did not start in time.");
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

/**
 * Starts the server for a test file: the one in REDIS_URL, or a throwaway instance on a free local port (no persistence, no
 * default port, its own empty directory).
 */
export async function startServer(): Promise<TestServer> {
  if (EXTERNAL_URL !== undefined) {
    const url = new URL(EXTERNAL_URL);
    const endpoint: Endpoint = {
      host: url.hostname,
      port: url.port === "" ? 6379 : Number(url.port),
      ...(url.password === "" ? {} : { password: decodeURIComponent(url.password) }),
    };
    await ready(endpoint, undefined);
    return { endpoint, external: true, stop: async () => undefined };
  }
  if (BINARY === undefined) throw new Error(NO_SERVER);
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const port = await freePort();
    const dir = mkdtempSync(join(tmpdir(), "rt-redis-"));
    const child = spawn(
      BINARY,
      [
        "--port",
        String(port),
        "--bind",
        "127.0.0.1",
        "--save",
        "",
        "--appendonly",
        "no",
        "--daemonize",
        "no",
        "--dir",
        dir,
      ],
      { stdio: "ignore" },
    );
    const endpoint: Endpoint = { host: "127.0.0.1", port };
    const stop = async (): Promise<void> => {
      if (child.exitCode === null) {
        const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
        child.kill("SIGTERM");
        const timer = setTimeout(() => child.kill("SIGKILL"), 2000);
        await exited;
        clearTimeout(timer);
      }
      rmSync(dir, { recursive: true, force: true });
    };
    try {
      await ready(endpoint, child);
      return { endpoint, external: false, stop };
    } catch (error) {
      await stop();
      if (attempt === 4) throw error;
    }
  }
  throw new Error("unreachable");
}

/** A key prefix no other test (or parallel run on a shared server) uses. */
export function uniquePrefix(label = "t"): string {
  return `${label}-${randomBytes(6).toString("hex")}`;
}

/** Deletes the keys under a prefix (for a shared server; a throwaway one is discarded). */
export async function clean(server: TestServer, prefix: string): Promise<void> {
  if (!server.external) return;
  const client = new RespClient(server.endpoint);
  try {
    let cursor = "0";
    do {
      const reply = (await client.sendCommand([
        "SCAN",
        cursor,
        "MATCH",
        `${prefix}:*`,
        "COUNT",
        "500",
      ])) as [string, string[]];
      cursor = reply[0];
      if (reply[1].length > 0) await client.sendCommand(["UNLINK", ...reply[1]]);
    } while (cursor !== "0");
  } finally {
    await client.close();
  }
}

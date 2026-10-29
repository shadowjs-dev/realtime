import { build } from "esbuild";

/**
 * Bundles one source module the way the package builds it for production (`__DEV__` off) and
 * imports the result. The tests run with `__DEV__` on, so the production messages can only be read
 * from a module built this way.
 *
 * @param entry Absolute path of a source module.
 * @param alias Workspace packages the module imports, mapped to their sources.
 * @returns The module's exports.
 */
export async function loadProduction<T>(
  entry: string,
  alias: Readonly<Record<string, string>> = {},
): Promise<T> {
  const built = await build({
    entryPoints: [entry],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    define: { __DEV__: "false", __SERVER__: "false", __TEST__: "false", __VERSION__: '"0"' },
    alias: { ...alias },
    logLevel: "silent",
  });
  const code = built.outputFiles[0]?.text ?? "";
  return (await import(
    /* @vite-ignore */ `data:text/javascript;base64,${Buffer.from(code).toString("base64")}`
  )) as T;
}

/**
 * The message of the error a call throws.
 *
 * @param run The call.
 * @returns The message; the test fails when nothing was thrown.
 */
export function messageOf(run: () => unknown): string {
  try {
    run();
  } catch (error) {
    if (error instanceof Error) return error.message;
    throw error;
  }
  throw new Error("the call did not throw");
}

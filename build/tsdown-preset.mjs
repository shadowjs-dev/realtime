import { readFileSync } from "node:fs";
import { defineConfig } from "tsdown";

/**
 * Shared tsdown preset: one ESM build per mode (dev / prod / server).
 * `__DEV__` is replaced at build time so dev-only code is dropped from prod bundles.
 * `unbundle` keeps one output file per source module (tsdown's `unbundle`). Without it, the modules of
 * an entry are merged into one file. A top-level call the bundler cannot prove pure (for example
 * `Object.assign(Component, { Part })`) then keeps that file, and everything it reaches, in every
 * application that imports one export of the package. With it, `sideEffects: false` lets the bundler
 * drop each unused module whole.
 * @param {{ entries?: Record<string, string>, serverEntries?: Record<string, string>, packageDir: string, unbundle?: boolean }} options
 */
export function createConfig({
  entries = { index: "src/index.ts" },
  serverEntries,
  packageDir,
  unbundle = true,
}) {
  const pkg = JSON.parse(readFileSync(`${packageDir}/package.json`, "utf8"));
  const define = (dev, server = false) => ({
    __DEV__: JSON.stringify(dev),
    __SERVER__: JSON.stringify(server),
    __TEST__: "false",
    __VERSION__: JSON.stringify(pkg.version),
  });
  const common = {
    format: "esm",
    platform: "neutral",
    target: "es2022",
    clean: false,
    sourcemap: true,
    unbundle,
  };
  const builds = [
    { ...common, entry: entries, outDir: "dist/dev", define: define(true), dts: true, clean: true },
    {
      ...common,
      entry: entries,
      outDir: "dist/prod",
      define: define(false),
      minify: true,
      dts: false,
      clean: true,
    },
  ];
  if (serverEntries !== undefined) {
    // Server condition builds: effects are inert, no DOM assumptions. Dev and prod variants.
    builds.push(
      {
        ...common,
        entry: serverEntries,
        outDir: "dist/server-dev",
        define: define(true, true),
        dts: false,
        clean: true,
      },
      {
        ...common,
        entry: serverEntries,
        outDir: "dist/server",
        define: define(false, true),
        minify: true,
        dts: false,
        clean: true,
      },
    );
  }
  return defineConfig(builds);
}

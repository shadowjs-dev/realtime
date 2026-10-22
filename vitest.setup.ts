import { afterEach, beforeEach, expect, vi } from "vitest";

// R-TEST-7: un-asserted console output fails the test. Tests that expect output
// must read `console.warn.mock.calls` themselves and then call `mockClear()`.
const methods = ["warn", "error"] as const;

beforeEach(() => {
  for (const method of methods) vi.spyOn(console, method).mockImplementation(() => undefined);
});

afterEach(() => {
  const leaked = methods.flatMap((method) =>
    vi.mocked(console[method]).mock.calls.map((args) => `console.${method}: ${args.join(" ")}`),
  );
  vi.restoreAllMocks();
  expect(leaked, "un-asserted console output").toEqual([]);
});

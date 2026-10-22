import { afterEach, describe, expect, it } from "vitest";
import { type Adapter, memoryAdapter } from "../src/server/index";
import { adapterConformance } from "../src/testing";

const stores = new Map<string, Adapter>();
afterEach(() => stores.clear());

// The shared adapter conformance suite (src/testing.ts) registers one test per id: RT-AD-01, RT-AD-02, RT-AD-03, RT-AD-04, RT-AD-05, RT-AD-06, RT-AD-07, RT-AD-08, RT-AD-09, RT-AD-10, RT-AD-20, RT-AD-21, RT-AD-22, RT-AD-23, RT-AD-24, RT-AD-25.
adapterConformance({
  runner: { describe, it, expect, afterEach: (body) => afterEach(body) },
  connect: (namespace) => {
    let store = stores.get(namespace);
    if (store === undefined) {
      store = memoryAdapter();
      stores.set(namespace, store);
    }
    return store;
  },
});

import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { redisAdapter } from "../src/redis";
import { adapterConformance } from "../src/testing";
import { RespClient } from "./support/resp";
import {
  AVAILABLE,
  clean,
  NO_SERVER,
  startServer,
  type TestServer,
  uniquePrefix,
} from "./support/server";

if (!AVAILABLE) {
  describe("redis adapter conformance", () => {
    it("[RDS-SERVER] a Redis or Valkey server is available", () => {
      throw new Error(NO_SERVER);
    });
  });
} else {
  let server: TestServer;
  const clients: RespClient[] = [];
  const prefixes = new Map<string, string>();

  beforeAll(async () => {
    server = await startServer();
  });
  afterAll(async () => {
    for (const prefix of prefixes.values()) await clean(server, prefix);
    await server.stop();
  });
  afterEach(async () => {
    await Promise.all(clients.splice(0).map((client) => client.close()));
  });

  adapterConformance({
    runner: { describe, it, expect, afterEach: (body) => afterEach(body) },
    connect: (namespace) => {
      let prefix = prefixes.get(namespace);
      if (prefix === undefined) {
        prefix = uniquePrefix("conf");
        prefixes.set(namespace, prefix);
      }
      const client = new RespClient(server.endpoint);
      clients.push(client);
      return redisAdapter({ client, prefix });
    },
  });
}

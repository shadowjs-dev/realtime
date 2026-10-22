import { describe, expect, it } from "vitest";
import {
  fromIoredis,
  fromNodeRedis,
  type IoredisLike,
  type IoredisSubscriberLike,
  type NodeRedisLike,
  type NodeRedisSubscriberLike,
} from "../src/redis";

// Structural fakes: they only check how the wrappers map the two libraries' calls. The adapter's behavior is tested against a
// real server in the other redis test files.

type Handler = (...args: never[]) => void;

class Emitter {
  readonly handlers = new Map<string, Handler[]>();
  on(event: string, listener: Handler): this {
    this.handlers.set(event, [...(this.handlers.get(event) ?? []), listener]);
    return this;
  }
  emit(event: string, ...args: unknown[]): void {
    for (const handler of this.handlers.get(event) ?? [])
      (handler as (...a: unknown[]) => void)(...args);
  }
}

class FakeIoredisSubscriber extends Emitter implements IoredisSubscriberLike {
  readonly log: string[] = [];
  quitFails = false;
  async subscribe(...topics: string[]): Promise<number> {
    this.log.push(`subscribe ${topics.join(",")}`);
    return 1;
  }
  async unsubscribe(...topics: string[]): Promise<number> {
    this.log.push(`unsubscribe ${topics.join(",")}`);
    return 0;
  }
  async quit(): Promise<string> {
    this.log.push("quit");
    if (this.quitFails) throw new Error("not connected");
    return "OK";
  }
  disconnect(): void {
    this.log.push("disconnect");
  }
}

class FakeIoredis implements IoredisLike {
  readonly calls: string[][] = [];
  readonly duplicated: FakeIoredisSubscriber[] = [];
  async call(command: string, ...args: string[]): Promise<unknown> {
    this.calls.push([command, ...args]);
    return "reply";
  }
  duplicate(): FakeIoredisSubscriber {
    const subscriber = new FakeIoredisSubscriber();
    this.duplicated.push(subscriber);
    return subscriber;
  }
}

describe("fromIoredis", () => {
  it("[RT-RD-80] maps sendCommand to call with the command split from its arguments", async () => {
    const fake = new FakeIoredis();
    const client = fromIoredis(fake);
    expect(await client.sendCommand(["EVALSHA", "abc", "1", "key", "arg"])).toBe("reply");
    expect(fake.calls).toEqual([["EVALSHA", "abc", "1", "key", "arg"]]);
  });

  it("[RT-RD-81] dispatches messages by topic, counts only the second ready as a reconnect, and forwards errors", async () => {
    const fake = new FakeIoredis();
    const subscriber = await fromIoredis(fake).createSubscriber?.();
    const connection = fake.duplicated[0];
    if (subscriber === undefined || connection === undefined) throw new Error("no subscriber");
    const got: string[] = [];
    const other: string[] = [];
    const reconnects: number[] = [];
    const errors: unknown[] = [];
    subscriber.onReconnect(() => reconnects.push(1));
    subscriber.onError?.((error) => errors.push(error));
    await subscriber.subscribe("t1", (message) => got.push(message));
    await subscriber.subscribe("t2", (message) => other.push(message));
    connection.emit("ready");
    connection.emit("message", "t1", "one");
    connection.emit("message", "t2", "two");
    connection.emit("message", "nope", "ignored");
    expect(got).toEqual(["one"]);
    expect(other).toEqual(["two"]);
    expect(reconnects).toEqual([]);
    connection.emit("ready");
    expect(reconnects).toEqual([1]);
    connection.emit("error", new Error("boom"));
    expect(errors).toHaveLength(1);
    await subscriber.subscribe("t1", (message) => other.push(`new:${message}`));
    connection.emit("message", "t1", "again");
    expect(got).toEqual(["one"]);
    expect(other).toEqual(["two", "new:again"]);
    await subscriber.unsubscribe("t1");
    connection.emit("message", "t1", "after");
    expect(other).toEqual(["two", "new:again"]);
    expect(connection.log).toEqual([
      "subscribe t1",
      "subscribe t2",
      "subscribe t1",
      "unsubscribe t1",
    ]);
  });

  it("[RT-RD-82] quits the duplicate on close, and disconnects when quit fails", async () => {
    const fake = new FakeIoredis();
    const first = await fromIoredis(fake).createSubscriber?.();
    await first?.close?.();
    expect(fake.duplicated[0]?.log).toEqual(["quit"]);
    const second = await fromIoredis(fake).createSubscriber?.();
    const connection = fake.duplicated[1];
    if (connection === undefined) throw new Error("no connection");
    connection.quitFails = true;
    await second?.close?.();
    expect(connection.log).toEqual(["quit", "disconnect"]);
  });
});

class FakeNodeSubscriber extends Emitter implements NodeRedisSubscriberLike {
  readonly log: string[] = [];
  dispatch: ((message: string, topic: string) => void) | undefined;
  async connect(): Promise<void> {
    this.log.push("connect");
    this.emit("ready");
  }
  async subscribe(
    topic: string,
    listener: (message: string, topic: string) => void,
  ): Promise<void> {
    this.log.push(`subscribe ${topic}`);
    this.dispatch = listener;
  }
  async unsubscribe(topic: string): Promise<void> {
    this.log.push(`unsubscribe ${topic}`);
  }
  async close(): Promise<void> {
    this.log.push("close");
  }
}

class FakeNodeRedis implements NodeRedisLike {
  readonly calls: string[][] = [];
  readonly duplicated: FakeNodeSubscriber[] = [];
  async sendCommand(args: string[]): Promise<unknown> {
    this.calls.push(args);
    return 7;
  }
  duplicate(): FakeNodeSubscriber {
    const subscriber = new FakeNodeSubscriber();
    this.duplicated.push(subscriber);
    return subscriber;
  }
}

describe("fromNodeRedis", () => {
  it("[RT-RD-83] passes commands through as a fresh array", async () => {
    const fake = new FakeNodeRedis();
    const args = ["PING"] as const;
    expect(await fromNodeRedis(fake).sendCommand(args)).toBe(7);
    expect(fake.calls).toEqual([["PING"]]);
    expect(fake.calls[0]).not.toBe(args);
  });

  it("[RT-RD-84] connects the duplicate, subscribes a topic once, dispatches by topic and counts reconnects", async () => {
    const fake = new FakeNodeRedis();
    const subscriber = await fromNodeRedis(fake).createSubscriber?.();
    const connection = fake.duplicated[0];
    if (subscriber === undefined || connection === undefined) throw new Error("no subscriber");
    const got: string[] = [];
    const reconnects: number[] = [];
    const errors: unknown[] = [];
    subscriber.onReconnect(() => reconnects.push(1));
    subscriber.onError?.((error) => errors.push(error));
    await subscriber.subscribe("t1", (message) => got.push(`a:${message}`));
    await subscriber.subscribe("t1", (message) => got.push(`b:${message}`));
    expect(connection.log).toEqual(["connect", "subscribe t1"]);
    connection.dispatch?.("hello", "t1");
    connection.dispatch?.("lost", "other");
    expect(got).toEqual(["b:hello"]);
    expect(reconnects).toEqual([]);
    connection.emit("ready");
    expect(reconnects).toEqual([1]);
    connection.emit("error", new Error("boom"));
    expect(errors).toHaveLength(1);
    await subscriber.unsubscribe("t1");
    await subscriber.unsubscribe("t1");
    expect(connection.log).toEqual(["connect", "subscribe t1", "unsubscribe t1"]);
    connection.dispatch?.("late", "t1");
    expect(got).toEqual(["b:hello"]);
    await subscriber.close?.();
    expect(connection.log.at(-1)).toBe("close");
  });

  it("[RT-RD-85] forgets a topic whose subscribe failed, so a retry subscribes again", async () => {
    const fake = new FakeNodeRedis();
    const subscriber = await fromNodeRedis(fake).createSubscriber?.();
    const connection = fake.duplicated[0];
    if (subscriber === undefined || connection === undefined) throw new Error("no subscriber");
    const original = connection.subscribe.bind(connection);
    let fail = true;
    connection.subscribe = async (topic, listener) => {
      if (fail) throw new Error("down");
      await original(topic, listener);
    };
    await expect(subscriber.subscribe("t1", () => undefined)).rejects.toThrow("down");
    fail = false;
    await subscriber.subscribe("t1", () => undefined);
    expect(connection.log).toEqual(["connect", "subscribe t1"]);
  });

  it("[RT-RD-86] closes with quit or disconnect when close is not there (older clients)", async () => {
    const fake = new FakeNodeRedis();
    const subscriber = await fromNodeRedis(fake).createSubscriber?.();
    const connection = fake.duplicated[0];
    if (subscriber === undefined || connection === undefined) throw new Error("no subscriber");
    Object.defineProperty(connection, "close", { value: undefined });
    Object.defineProperty(connection, "quit", {
      value: async () => {
        connection.log.push("quit");
      },
    });
    await subscriber.close?.();
    expect(connection.log.at(-1)).toBe("quit");
  });
});

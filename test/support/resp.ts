import { connect, type Socket } from "node:net";
import type { RedisClient, RedisSubscriber } from "../../src/redis";

/**
 * A small real RESP2 client for the tests: a socket that speaks the Redis protocol, with request/response commands (pipelined
 * when issued without waiting) and a subscriber mode that reconnects. It exists so the adapter is tested against a real server
 * without a Redis library; the conformance run exercises it as much as the adapter.
 */

/** An error reply from the server. */
export class RespError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RespError";
  }
}

type Reply = string | number | null | RespError | Reply[];

/** Parses one reply at `at`; undefined when the buffer does not hold a whole one yet. */
function parse(buffer: Buffer, at: number): [Reply, number] | undefined {
  if (at >= buffer.length) return undefined;
  const end = buffer.indexOf("\r\n", at, "latin1");
  if (end === -1) return undefined;
  const kind = String.fromCharCode(buffer[at] as number);
  const line = buffer.toString("utf8", at + 1, end);
  const next = end + 2;
  switch (kind) {
    case "+":
      return [line, next];
    case "-":
      return [new RespError(line), next];
    case ":":
      return [Number(line), next];
    case "$": {
      const length = Number(line);
      if (length === -1) return [null, next];
      if (buffer.length < next + length + 2) return undefined;
      return [buffer.toString("utf8", next, next + length), next + length + 2];
    }
    case "*": {
      const count = Number(line);
      if (count === -1) return [null, next];
      const items: Reply[] = [];
      let pos = next;
      for (let item = 0; item < count; item += 1) {
        const parsed = parse(buffer, pos);
        if (parsed === undefined) return undefined;
        items.push(parsed[0]);
        pos = parsed[1];
      }
      return [items, pos];
    }
    default:
      throw new Error(`unexpected RESP type "${kind}"`);
  }
}

function encode(args: readonly string[]): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
  for (const arg of args) {
    const body = Buffer.from(arg, "utf8");
    parts.push(Buffer.from(`$${body.length}\r\n`), body, Buffer.from("\r\n"));
  }
  return Buffer.concat(parts);
}

/** Splits a stream into replies. */
class Reader {
  #buffer: Buffer = Buffer.alloc(0);
  readonly #chunks: Buffer[] = [];

  push(chunk: Buffer): Reply[] {
    this.#chunks.push(chunk);
    this.#buffer = Buffer.concat([this.#buffer, ...this.#chunks.splice(0)]);
    const out: Reply[] = [];
    let at = 0;
    for (;;) {
      const parsed = parse(this.#buffer, at);
      if (parsed === undefined) break;
      out.push(parsed[0]);
      at = parsed[1];
    }
    this.#buffer = this.#buffer.subarray(at);
    return out;
  }
}

/** Where a server listens. */
export interface Endpoint {
  readonly host: string;
  readonly port: number;
  readonly password?: string;
}

function open(endpoint: Endpoint): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: endpoint.host, port: endpoint.port });
    socket.setNoDelay(true);
    socket.once("connect", () => {
      socket.off("error", reject);
      resolve(socket);
    });
    socket.once("error", reject);
  });
}

interface Waiter {
  readonly resolve: (reply: unknown) => void;
  readonly reject: (error: Error) => void;
}

/** A connection that runs commands in order, connecting on demand. */
class Connection {
  #socket: Socket | undefined;
  #connecting: Promise<Socket> | undefined;
  readonly #waiters: Waiter[] = [];
  #closed = false;

  readonly endpoint: Endpoint;

  constructor(endpoint: Endpoint) {
    this.endpoint = endpoint;
  }

  async #socketReady(): Promise<Socket> {
    if (this.#closed) throw new Error("the connection is closed.");
    if (this.#socket !== undefined) return this.#socket;
    this.#connecting ??= (async () => {
      const socket = await open(this.endpoint);
      const reader = new Reader();
      socket.on("data", (chunk: Buffer) => {
        for (const reply of reader.push(chunk)) {
          const waiter = this.#waiters.shift();
          if (waiter === undefined) continue;
          if (reply instanceof RespError) waiter.reject(reply);
          else waiter.resolve(reply);
        }
      });
      const lost = (): void => {
        if (this.#socket === socket) this.#socket = undefined;
        for (const waiter of this.#waiters.splice(0)) waiter.reject(new Error("connection lost"));
      };
      socket.on("close", lost);
      socket.on("error", () => undefined);
      this.#socket = socket;
      if (this.endpoint.password !== undefined) {
        socket.write(encode(["AUTH", this.endpoint.password]));
        await new Promise<void>((resolve, reject) =>
          this.#waiters.push({ resolve: () => resolve(), reject }),
        );
      }
      return socket;
    })();
    try {
      return await this.#connecting;
    } finally {
      this.#connecting = undefined;
    }
  }

  async send(args: readonly string[]): Promise<unknown> {
    const socket = await this.#socketReady();
    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject });
      socket.write(encode(args));
    });
  }

  /** Drops the socket as a network failure would. */
  sever(): void {
    this.#socket?.destroy();
  }

  close(): void {
    this.#closed = true;
    this.#socket?.destroy();
    for (const waiter of this.#waiters.splice(0)) waiter.reject(new Error("connection closed"));
  }
}

/** A subscriber connection that survives a lost socket like ioredis does: it reconnects, subscribes again, tells its listeners. */
export class RespSubscriber implements RedisSubscriber {
  /** Milliseconds to wait before reconnecting (tests raise it to publish during the outage). */
  reconnectDelayMs = 20;
  /** The server-side id of the current connection (for `CLIENT KILL ID`). */
  clientId = 0;
  readonly #topics = new Map<string, (message: string) => void>();
  readonly #confirm = new Map<string, (() => void)[]>();
  readonly #reconnects = new Set<() => void>();
  readonly #errors = new Set<(error: unknown) => void>();
  #socket: Socket | undefined;
  #closed = false;
  #connected: Promise<void>;
  /** How many times the connection came back. */
  reconnected = 0;

  readonly endpoint: Endpoint;

  constructor(endpoint: Endpoint) {
    this.endpoint = endpoint;
    this.#connected = this.#connect();
    this.#connected.catch(() => undefined);
  }

  async #connect(): Promise<void> {
    const socket = await open(this.endpoint);
    if (this.#closed) {
      socket.destroy();
      return;
    }
    const reader = new Reader();
    let probe: ((reply: Reply) => void) | undefined;
    socket.on("data", (chunk: Buffer) => {
      for (const reply of reader.push(chunk)) {
        if (Array.isArray(reply) && typeof reply[0] === "string") {
          const [kind, topic, payload] = reply;
          if (kind === "message" && typeof topic === "string" && typeof payload === "string")
            this.#topics.get(topic)?.(payload);
          else if ((kind === "subscribe" || kind === "unsubscribe") && typeof topic === "string")
            for (const done of this.#confirm.get(`${kind}:${topic}`)?.splice(0) ?? []) done();
        } else if (probe !== undefined) {
          const waiting = probe;
          probe = undefined;
          waiting(reply);
        }
      }
    });
    socket.on("error", (error) => {
      for (const handler of this.#errors) handler(error);
    });
    socket.on("close", () => {
      if (this.#socket === socket) this.#socket = undefined;
      if (this.#closed) return;
      this.#connected = this.#reconnect();
    });
    if (this.endpoint.password !== undefined) {
      socket.write(encode(["AUTH", this.endpoint.password]));
      await new Promise<Reply>((resolve) => {
        probe = resolve;
      });
    }
    socket.write(encode(["CLIENT", "ID"]));
    this.clientId = Number(
      await new Promise<Reply>((resolve) => {
        probe = resolve;
      }),
    );
    this.#socket = socket;
    if (this.#topics.size > 0) {
      const topics = [...this.#topics.keys()];
      const confirmed = topics.map((topic) => this.#waitFor(`subscribe:${topic}`));
      socket.write(encode(["SUBSCRIBE", ...topics]));
      await Promise.all(confirmed);
    }
  }

  async #reconnect(): Promise<void> {
    while (!this.#closed) {
      await new Promise((resolve) => setTimeout(resolve, this.reconnectDelayMs));
      try {
        await this.#connect();
        if (this.#socket !== undefined) {
          // The subscriptions are back (see `#connect`) before the reconnect is announced, as the libraries do.
          this.reconnected += 1;
          for (const handler of [...this.#reconnects]) handler();
          return;
        }
      } catch {
        // Try again.
      }
    }
  }

  #waitFor(key: string): Promise<void> {
    return new Promise((resolve) => {
      const waiting = this.#confirm.get(key) ?? [];
      waiting.push(resolve);
      this.#confirm.set(key, waiting);
    });
  }

  async subscribe(topic: string, listener: (message: string) => void): Promise<void> {
    this.#topics.set(topic, listener);
    await this.#connected;
    const confirmed = this.#waitFor(`subscribe:${topic}`);
    this.#socket?.write(encode(["SUBSCRIBE", topic]));
    await confirmed;
  }

  async unsubscribe(topic: string): Promise<void> {
    this.#topics.delete(topic);
    if (this.#socket === undefined) return;
    const confirmed = this.#waitFor(`unsubscribe:${topic}`);
    this.#socket.write(encode(["UNSUBSCRIBE", topic]));
    await confirmed;
  }

  onReconnect(handler: () => void): () => void {
    this.#reconnects.add(handler);
    return () => {
      this.#reconnects.delete(handler);
    };
  }

  onError(handler: (error: unknown) => void): () => void {
    this.#errors.add(handler);
    return () => {
      this.#errors.delete(handler);
    };
  }

  /** How many reconnect handlers are registered. */
  get handlerCount(): number {
    return this.#reconnects.size + this.#errors.size;
  }

  /** The topics currently subscribed. */
  get topics(): readonly string[] {
    return [...this.#topics.keys()];
  }

  /** Drops the connection as a network failure would; the subscriber reconnects by itself. */
  sever(): void {
    this.#socket?.destroy();
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#socket?.destroy();
  }
}

/** A Redis client over RESP2: commands, and subscribers on their own connections. */
export class RespClient implements RedisClient {
  readonly #connection: Connection;
  readonly subscribers: RespSubscriber[] = [];

  readonly endpoint: Endpoint;

  constructor(endpoint: Endpoint) {
    this.endpoint = endpoint;
    this.#connection = new Connection(endpoint);
  }

  sendCommand(args: readonly string[]): Promise<unknown> {
    return this.#connection.send(args);
  }

  createSubscriber(): RespSubscriber {
    const subscriber = new RespSubscriber(this.endpoint);
    this.subscribers.push(subscriber);
    return subscriber;
  }

  /** Drops the command connection (the next command connects again). */
  sever(): void {
    this.#connection.sever();
  }

  /** Closes the command connection and every subscriber this client created. */
  async close(): Promise<void> {
    this.#connection.close();
    await Promise.all(this.subscribers.splice(0).map((subscriber) => subscriber.close()));
  }
}

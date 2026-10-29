import { afterEach, describe, expect, it } from "vitest";
import { ChannelLink, type LinkHost } from "../src/cloudflare/link";
import type { ClientSocket } from "../src/cloudflare/types";
import type { LinkFrame } from "../src/cloudflare/wire";
import { RealtimeError } from "../src/errors";
import type { StoredMessage } from "../src/protocol";

/** A client socket the test drives by hand. */
class ScriptedSocket implements ClientSocket {
  accepted = false;
  readonly sent: string[] = [];
  closes: number[] = [];
  closeAttempts = 0;
  /** Make `send` and `close` throw, as the runtime does for a socket used from another request. */
  foreign = false;
  #message: ((event: { readonly data: unknown }) => void) | undefined;
  #close: ((event: { readonly code?: number }) => void) | undefined;
  #error: ((event: unknown) => void) | undefined;
  accept(): void {
    this.accepted = true;
  }
  send(data: string): void {
    if (this.foreign) throw new Error("Cannot perform I/O on behalf of a different request");
    this.sent.push(data);
  }
  close(code?: number): void {
    this.closeAttempts += 1;
    if (this.foreign) throw new Error("Cannot perform I/O on behalf of a different request");
    this.closes.push(code ?? 1005);
  }
  addEventListener(type: "message", listener: (event: { readonly data: unknown }) => void): void;
  addEventListener(type: "close", listener: (event: { readonly code?: number }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(type: string, listener: (event: never) => void): void {
    if (type === "message") this.#message = listener as (event: { readonly data: unknown }) => void;
    else if (type === "close")
      this.#close = listener as (event: { readonly code?: number }) => void;
    else this.#error = listener as (event: unknown) => void;
  }
  emit(frame: LinkFrame | string | ArrayBuffer): void {
    this.#message?.({
      data:
        typeof frame === "string" || frame instanceof ArrayBuffer ? frame : JSON.stringify(frame),
    });
  }
  drop(): void {
    this.#close?.({});
  }
  fail(): void {
    this.#error?.({});
  }
}

/** A host whose clock and timers the test controls, and whose sockets are scripted. */
class ScriptedHost implements LinkHost {
  readonly reconnectMinMs = 100;
  readonly reconnectMaxMs = 1000;
  readonly keepAliveMs = 1000;
  readonly connectTimeoutMs = 500;
  readonly maxQueued: number = 100;
  clock = 1_000_000;
  readonly sockets: ScriptedSocket[] = [];
  readonly opened: (number | undefined)[] = [];
  readonly reports: unknown[] = [];
  readonly forgotten: ChannelLink[] = [];
  readonly tasks: { at: number; run: () => void; live: boolean }[] = [];
  readonly intervals: { every: number; run: () => void; live: boolean }[] = [];
  /** When true the next `open` never answers. */
  hang = false;
  /** When true timers never fire (the request that created them ended). */
  deadContext = false;
  now = (): number => this.clock;
  random = (): number => 0.999;
  open(_channel: string, after: number | undefined): Promise<ClientSocket> {
    this.opened.push(after);
    if (this.hang) return new Promise(() => undefined);
    const socket = new ScriptedSocket();
    this.sockets.push(socket);
    return Promise.resolve(socket);
  }
  after(ms: number, run: () => void): () => void {
    const task = { at: this.clock + ms, run, live: true };
    this.tasks.push(task);
    return () => {
      task.live = false;
    };
  }
  every(every: number, run: () => void): () => void {
    const interval = { every, run, live: true };
    this.intervals.push(interval);
    return () => {
      interval.live = false;
    };
  }
  report(error: unknown): void {
    this.reports.push(error);
  }
  forget(link: ChannelLink): void {
    this.forgotten.push(link);
  }
  /** Moves the clock and runs the timers that came due (unless the context is dead). */
  advance(ms: number): void {
    this.clock += ms;
    if (this.deadContext) return;
    for (const task of this.tasks)
      if (task.live && task.at <= this.clock) {
        task.live = false;
        task.run();
      }
  }
  /** What the listeners' loops do in their own live requests. */
  nudge(): void {
    for (const interval of this.intervals) if (interval.live) interval.run();
  }
}

const flush = async (): Promise<void> => {
  for (let at = 0; at < 6; at += 1) await Promise.resolve();
};
const message = (seq: number): StoredMessage => ({ seq, event: "e", data: String(seq), at: 1 });
const ready = (head: number): LinkFrame => ({ t: "ready", head, oldest: 0 });

const links: ChannelLink[] = [];
afterEach(() => {
  for (const link of links.splice(0)) link.dispose(new Error("test over"));
});
function linkOn(host: ScriptedHost): ChannelLink {
  const link = new ChannelLink("room", host);
  links.push(link);
  return link;
}

describe("channel link: a socket whose request has ended", () => {
  it("[RT-CF-140] a retry timer that can never fire is replaced by the next health check of a live listener", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    const seen: number[] = [];
    link.addMessages((item) => seen.push(item.seq));
    await flush();
    host.sockets[0]?.emit(ready(0));
    await flush();
    expect(link.state).toBe("open");
    host.sockets[0]?.drop();
    expect(link.state).toBe("backoff");
    host.deadContext = true;
    host.advance(5000);
    expect(link.state).toBe("backoff");
    expect(host.sockets).toHaveLength(1);
    host.nudge();
    await flush();
    expect(host.sockets).toHaveLength(2);
    host.sockets[1]?.emit(ready(0));
    host.sockets[1]?.emit({ t: "msg", m: message(1) });
    await flush();
    expect(link.state).toBe("open");
    expect(seen).toEqual([1]);
    expect(host.opened).toEqual([undefined, 0]);
  });

  it("[RT-CF-141] a connection attempt that never answers is restarted by the health check once twice the timeout has passed", async () => {
    const host = new ScriptedHost();
    host.hang = true;
    const link = linkOn(host);
    link.addMessages(() => undefined);
    await flush();
    host.deadContext = true;
    host.clock += 400;
    host.nudge();
    expect(host.opened).toHaveLength(1);
    host.clock += 700;
    host.hang = false;
    host.nudge();
    await flush();
    expect(host.opened).toHaveLength(2);
    expect(host.sockets).toHaveLength(1);
  });

  it("[RT-CF-142] an open socket that went silent is declared dead after three missed pings, and replaced", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    link.addMessages(() => undefined);
    await flush();
    host.sockets[0]?.emit(ready(7));
    await flush();
    host.clock += 2500;
    host.nudge();
    expect(link.state).toBe("open");
    expect(host.sockets[0]?.sent.at(-1)).toBe("ping");
    host.clock += 1000;
    host.nudge();
    expect(link.state).toBe("backoff");
    host.deadContext = false;
    host.advance(2000);
    await flush();
    expect(host.sockets).toHaveLength(2);
    expect(host.opened[1]).toBe(7);
  });

  it("[RT-CF-143] a socket another request cannot close is handed to the request that opened it, which closes it", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    const first = link.addMessages(() => undefined);
    await flush();
    const socket = host.sockets[0];
    socket?.emit(ready(0));
    await flush();
    if (socket) socket.foreign = true;
    const attempts = socket?.closeAttempts ?? 0;
    link.remove(first);
    // The direct close was refused; the opener's waiting code tries again (here it runs where closing is allowed).
    if (socket) socket.foreign = false;
    await flush();
    expect(socket?.closeAttempts).toBe(attempts + 2);
    expect(socket?.closes).toEqual([1000]);
    expect(host.forgotten).toEqual([link]);
  });

  it("[RT-CF-144] a socket that arrives after its attempt was given up is closed, not used", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    const late = new ScriptedSocket();
    host.open = (_channel, after) => {
      host.opened.push(after);
      return new Promise((resolve) => setTimeout(() => resolve(late), 20));
    };
    const only = link.addMessages(() => undefined);
    link.remove(only);
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(late.accepted).toBe(false);
    expect(late.closes).toEqual([1000]);
    expect(link.state).toBe("disposed");
  });
});

describe("channel link: frames", () => {
  it("[RT-CF-145] ignores binary data, garbage, unknown and malformed frames, and keeps delivering", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    const seen: number[] = [];
    link.addMessages((item) => seen.push(item.seq));
    await flush();
    const socket = host.sockets[0];
    socket?.emit(ready(0));
    socket?.emit(new ArrayBuffer(8));
    socket?.emit("not json");
    socket?.emit("[1,2]");
    socket?.emit('{"t":"nothing"}');
    socket?.emit('{"t":"msg","m":{"seq":"1"}}');
    socket?.emit('{"t":"pres","op":"join","member":{}}');
    socket?.emit({ t: "msg", m: message(1) });
    await flush();
    expect(seen).toEqual([1]);
    expect(link.state).toBe("open");
  });

  it("[RT-CF-146] a duplicate or older message on the socket is not delivered twice, per listener", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    const first: number[] = [];
    const second: number[] = [];
    link.addMessages((item) => first.push(item.seq));
    await flush();
    const socket = host.sockets[0];
    socket?.emit(ready(0));
    await flush();
    socket?.emit({ t: "msg", m: message(1) });
    socket?.emit({ t: "msg", m: message(2) });
    await flush();
    link.addMessages((item) => second.push(item.seq));
    socket?.emit({ t: "msg", m: message(2) });
    socket?.emit({ t: "msg", m: message(1) });
    socket?.emit({ t: "msg", m: message(3) });
    await flush();
    expect(first).toEqual([1, 2, 3]);
    expect(second).toEqual([3]);
  });

  it("[RT-CF-147] a bye frame reconnects at once with backoff, resuming after the last seq", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    link.addMessages(() => undefined);
    await flush();
    host.sockets[0]?.emit(ready(0));
    host.sockets[0]?.emit({ t: "msg", m: message(5) });
    host.sockets[0]?.emit({ t: "bye", why: "moving" });
    await flush();
    expect(link.state).toBe("backoff");
    host.advance(2000);
    await flush();
    expect(host.opened).toEqual([undefined, 5]);
  });

  it("[RT-CF-148] the backoff grows with failures and stays under the ceiling", async () => {
    const host = new ScriptedHost();
    const link = linkOn(host);
    link.addMessages(() => undefined);
    const delays: number[] = [];
    for (let attempt = 0; attempt < 6; attempt += 1) {
      await flush();
      const socket = host.sockets.at(-1);
      const before = host.clock;
      socket?.fail();
      const retry = host.tasks.filter((task) => task.live).at(-1);
      delays.push((retry?.at ?? before) - before);
      host.advance(2000);
    }
    expect(delays).toEqual([99, 199, 399, 799, 999, 999]);
  });
});

class SmallHost extends ScriptedHost {
  override readonly maxQueued: number = 3;
}

describe("channel link: a listener queue that overflows", () => {
  it("[RT-R2-5] a presence listener that lost events is told so and resynced to the true member set", async () => {
    const host = new SmallHost();
    const link = linkOn(host);
    const view = new Map<string, number>();
    link.addPresence((event) => {
      if (event.op === "leave") view.delete(event.member.id);
      else view.set(event.member.id, event.member.v);
    });
    await flush();
    host.sockets[0]?.emit(ready(0));
    await flush();
    const member = (id: string, v = 1) => ({ id, s: null, v });
    // Five changes arrive in one tick; the queue holds three.
    for (const id of ["a", "b", "c", "d"])
      host.sockets[0]?.emit({ t: "pres", op: "join", member: member(id) });
    host.sockets[0]?.emit({ t: "pres", op: "leave", member: member("a") });
    host.sockets[0]?.emit({ t: "pres", op: "set", member: member("b", 2) });
    await flush();
    expect(host.reports.filter((item) => item instanceof RealtimeError)).toHaveLength(1);
    expect(Object.fromEntries(view)).toEqual({ b: 2, c: 1, d: 1 });
  });

  it("[RT-R2-5] a message listener that lost messages is reported, once per overflow", async () => {
    const host = new SmallHost();
    const link = linkOn(host);
    const seen: number[] = [];
    link.addMessages((item) => seen.push(item.seq));
    await flush();
    host.sockets[0]?.emit(ready(0));
    await flush();
    for (let seq = 1; seq <= 8; seq += 1) host.sockets[0]?.emit({ t: "msg", m: message(seq) });
    await flush();
    expect(seen).toEqual([1, 2, 3]);
    expect(host.reports).toHaveLength(1);
    expect(host.reports[0]).toBeInstanceOf(RealtimeError);
  });
});

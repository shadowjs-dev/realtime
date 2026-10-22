import type { Hub } from "../../src/server/index";
import type { HubConnection } from "../../src/server/hub";
import type { Identity } from "../../src/server/rules";

/** One socket the client opened, with the controls a test needs to break it. */
export interface FakeSocket {
  readonly url: string;
  readonly readyState: number;
  /** The text frames the client sent, in order. */
  readonly sent: string[];
  /** The code the socket closed with, once it did. */
  readonly closedWith: number | undefined;
  /** The hub connection behind it, once the upgrade succeeded. */
  readonly connection: HubConnection | undefined;
  /** The network drops: no close handshake, the client sees 1006. */
  drop(code?: number): void;
  /** The server closes it with a code. */
  closeFromServer(code: number, reason?: string): void;
  /** Frames stop flowing both ways (a dead link that never closes). */
  stall(): void;
  /** Only frames from the server to the client are held back; what the client sends still arrives. */
  stallInbound(): void;
  /** Delivers one server frame to the client as if the hub had sent it. */
  inject(frame: Record<string, unknown>): void;
  /** Delivers raw text to the client as one message. */
  injectRaw(text: string): void;
  /** Delivers the frames held back since `stall`. */
  resume(): void;
}

/** What a test controls about the sockets of one factory. */
export interface SocketFactory {
  /** A drop-in for the global `WebSocket`. */
  readonly Socket: typeof WebSocket;
  /** Every socket opened so far, oldest first. */
  readonly sockets: FakeSocket[];
  /** The sockets that are open or opening. */
  live(): FakeSocket[];
  /** Forgets the sockets that are closed (a long test would otherwise keep every one it ever opened). */
  prune(): void;
  /** Makes the next `count` upgrades fail at once (close 1006 without an open). */
  refuseNext(count: number): void;
  /** Makes every upgrade fail until `allow()`. */
  refuseAll(): void;
  allow(): void;
  /** Makes upgrades wait for `proceed()` before they complete. */
  hold(): void;
  proceed(): void;
}

/**
 * A `WebSocket` whose other end is a hub in the same process, with controls to drop, close, stall and refuse. The client under
 * test runs unchanged; nothing about the hub is faked.
 */
export function socketFactory(hub: () => Hub, identity: Identity = {}): SocketFactory {
  const sockets: FakeSocket[] = [];
  let refusals = 0;
  let refuseAll = false;
  let held: (() => void)[] | undefined;

  class Fake {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    onopen: ((event: Event) => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onclose: ((event: CloseEvent) => void) | null = null;
    onerror: ((event: Event) => void) | null = null;
    readyState = 0;
    readonly sent: string[] = [];
    connection: HubConnection | undefined;
    closedWith: number | undefined;
    private heldBytes = 0;
    private stalled = false;
    private inboundOnly = false;
    private held: (() => void)[] = [];

    readonly url: string;

    constructor(url: string) {
      this.url = url;
      sockets.push(this as unknown as FakeSocket);
      const open = (): void => {
        if (this.readyState !== 0) return;
        if (refuseAll || refusals > 0) {
          refusals = Math.max(0, refusals - 1);
          this.finish(1006, "");
          return;
        }
        this.readyState = 1;
        this.connection = hub().connect(
          {
            buffered: () => this.heldBytes,
            send: (frame) => {
              if (this.readyState !== 1) return false;
              this.toClient(frame);
              return true;
            },
            close: (code, reason) => this.finish(code, reason),
          },
          identity,
        );
        this.onopen?.(new Event("open"));
      };
      queueMicrotask(() => {
        if (held === undefined) open();
        else held.push(open);
      });
    }

    private toClient(frame: string): void {
      const deliver = (): void => {
        if (this.readyState === 1) this.onmessage?.(new MessageEvent("message", { data: frame }));
      };
      if (this.stalled) {
        this.heldBytes += frame.length;
        this.held.push(deliver);
      } else queueMicrotask(deliver);
    }

    inject(frame: Record<string, unknown>): void {
      this.toClient(JSON.stringify(frame));
    }

    injectRaw(text: string): void {
      this.toClient(text);
    }

    send(text: string): void {
      if (this.readyState !== 1) throw new DOMException("not open", "InvalidStateError");
      this.sent.push(text);
      const connection = this.connection;
      if (connection === undefined) return;
      if (this.stalled && !this.inboundOnly) this.held.push(() => connection.receive(text));
      else queueMicrotask(() => connection.receive(text));
    }

    close(code = 1000): void {
      if (this.readyState >= 2) return;
      this.finish(code, "");
    }

    drop(code = 1006): void {
      this.finish(code, "");
    }

    closeFromServer(code: number, reason = ""): void {
      this.finish(code, reason);
    }

    stall(): void {
      this.stalled = true;
      this.inboundOnly = false;
    }

    stallInbound(): void {
      this.stalled = true;
      this.inboundOnly = true;
    }

    resume(): void {
      this.stalled = false;
      this.inboundOnly = false;
      this.heldBytes = 0;
      for (const run of this.held.splice(0)) queueMicrotask(run);
    }

    private finish(code: number, reason: string): void {
      if (this.readyState === 3) return;
      this.readyState = 3;
      this.closedWith = code;
      this.held = [];
      this.heldBytes = 0;
      this.connection?.closed();
      const event = ({ type: "close", code, reason, wasClean: code !== 1006 }) as CloseEvent;
      queueMicrotask(() => this.onclose?.(event));
    }
  }

  return {
    Socket: Fake as unknown as typeof WebSocket,
    sockets,
    live: () => sockets.filter((socket) => socket.readyState < 2),
    prune() {
      const open = sockets.filter((socket) => socket.readyState < 3);
      sockets.length = 0;
      sockets.push(...open);
    },
    refuseNext(count) {
      refusals = count;
    },
    refuseAll() {
      refuseAll = true;
    },
    allow() {
      refuseAll = false;
      refusals = 0;
    },
    hold() {
      held = [];
    },
    proceed() {
      const pending = held ?? [];
      held = undefined;
      for (const run of pending) run();
    },
  };
}

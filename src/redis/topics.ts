import type { Unsubscribe } from "../adapter";
import type { Member, PresenceEvent, StoredMessage } from "../protocol";
import type { RedisSubscriber } from "./client";

/**
 * Where a failure the adapter handled came from.
 * @since 1.0.0
 */
export interface RedisErrorContext {
  /** The step that failed, for example `"parse"`, `"listener"`, `"reconnect"` or `"subscriber"`. */
  readonly where: string;
  /** The channel it concerned, when it concerned one. */
  readonly channel?: string;
}

/** What the subscription manager needs from the adapter that owns it. */
export interface TopicsDeps {
  /** The shared subscriber connection (opened on first use). */
  subscriber(): Promise<RedisSubscriber>;
  /** The retained messages after `after`, at most `limit`, oldest first. */
  history(channel: string, after: number, limit: number): Promise<readonly StoredMessage[]>;
  /** The live presence members of a channel. */
  members(channel: string): Promise<readonly Member[]>;
  /** Extends the lifetime of a channel's numbering. */
  keepAlive(channel: string): Promise<void>;
  /** Parses a pub/sub payload; undefined (after reporting) when it is malformed. */
  parseMessage(text: string, channel: string): StoredMessage | undefined;
  parsePresence(text: string, channel: string): PresenceEvent | undefined;
  messageTopic(channel: string): string;
  presenceTopic(channel: string): string;
  report(error: unknown, context: RedisErrorContext): void;
  /** Turns a failure into the error the adapter's callers see. */
  fail(error: unknown): Error;
  /** Milliseconds between keep-alive rounds. */
  keepAliveMs: number;
}

/** What the manager returns. */
export interface Topics {
  messages(channel: string, listener: (message: StoredMessage) => void): Promise<Unsubscribe>;
  presence(channel: string, listener: (event: PresenceEvent) => void): Promise<Unsubscribe>;
  /** Unsubscribes everything and stops the timers. */
  close(): Promise<void>;
  /** The number of topics currently subscribed (for tests and diagnostics). */
  readonly size: number;
}

/** Live messages held while a recovery reads history; a larger burst falls back to reading history again. */
const CATCH_UP_BUFFER = 1000;
/** Messages read per history page during recovery. */
const CATCH_UP_PAGE = 500;
/** Delays between attempts to recover after a reconnect. */
const RETRY_DELAYS_MS: readonly number[] = [50, 250, 1000];
/** Topics subscribed again at once after a reconnect. */
const RESUBSCRIBE_BATCH = 64;
/** Members tracked per presence channel; a larger channel is delivered unfiltered and not reconciled after a reconnect. */
const MAX_TRACKED_MEMBERS = 5000;

type MessageListener = (message: StoredMessage) => void;
type PresenceListener = (event: PresenceEvent) => void;

interface TopicBase {
  readonly channel: string;
  readonly topic: string;
  readonly handler: (text: string) => void;
  ready: Promise<void>;
  dead: boolean;
}

interface MessageTopic extends TopicBase {
  readonly kind: "message";
  readonly listeners: Set<MessageListener>;
  /** The highest seq handed to the listeners: nothing at or below it is delivered again. */
  last: number;
  /** When the message at `last` was accepted: a lower seq accepted later is a restarted numbering, not a repeat. */
  lastAt: number;
  /** True while history is being read to fill a gap: live messages wait in `buffer`. */
  catching: boolean;
  /** Set when a reconnect happened while a recovery was running: it reads once more. */
  rerun: boolean;
  buffer: StoredMessage[];
  overflow: boolean;
}

interface PresenceTopic extends TopicBase {
  readonly kind: "presence";
  readonly listeners: Set<PresenceListener>;
  /** The members as the listeners know them, by id. */
  members: Map<string, Member>;
  /** False when the channel is too large to track: events are then delivered unfiltered and not reconciled. */
  tracked: boolean;
  /** Events received while the state is being read; applied on top of it when it arrives. */
  pending: PresenceEvent[] | undefined;
}

type Topic = MessageTopic | PresenceTopic;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Applies an event to a member map; true when it changed the map (a stale or repeated event does not). */
function apply(members: Map<string, Member>, event: PresenceEvent): boolean {
  const known = members.get(event.member.id);
  if (event.op === "leave") return known !== undefined && members.delete(event.member.id);
  if (known !== undefined && known.v >= event.member.v) return false;
  members.set(event.member.id, event.member);
  return true;
}

/**
 * The shared subscriber connection of one adapter: topics are refcounted (one SUBSCRIBE per topic however many listeners), a
 * listener sees strictly increasing seqs and never a repeat, and after a reconnect the topics are subscribed again and what the
 * connection missed is read back from history (messages) or from the member list (presence).
 */
export function createTopics(deps: TopicsDeps): Topics {
  const topics = new Map<string, Topic>();
  let closed = false;
  let timer: ReturnType<typeof setInterval> | undefined;
  let reconnecting = false;
  let again = false;
  let connected: RedisSubscriber | undefined;
  /** Removes the hooks this manager put on the subscriber (a subscriber the caller passed outlives the adapter). */
  const unhook: (() => void)[] = [];

  function reportTo(error: unknown, topic: Topic, where: string): void {
    deps.report(error, { where, channel: topic.channel });
  }

  // --- delivery ------------------------------------------------------------------------------------------------------

  function emit(topic: MessageTopic, message: StoredMessage): void {
    if (topic.dead) return;
    // Redis stamps `at` itself, so a repeat carries the time it first had. A lower seq with a later time means the keys were
    // lost (a flush, a failover to a replica that lacked them) and numbering began again: dropping it would silence the
    // channel until the new numbers pass the old ones. The hub tells its subscribers `reset` when it sees one.
    if (message.seq <= topic.last && message.at <= topic.lastAt) return;
    topic.last = message.seq;
    topic.lastAt = Math.max(topic.lastAt, message.at);
    for (const listener of [...topic.listeners]) {
      try {
        listener(message);
      } catch (error) {
        reportTo(error, topic, "listener");
      }
    }
  }

  function onMessage(topic: MessageTopic, text: string): void {
    const message = deps.parseMessage(text, topic.channel);
    if (message === undefined || topic.dead) return;
    if (topic.catching || (topic.last > 0 && message.seq > topic.last + 1)) {
      // Either history is being read, or this message jumped ahead of the cursor (a publish this connection missed): hold it
      // back and fill the gap from history first, so the listeners see every retained message in order.
      if (topic.buffer.length >= CATCH_UP_BUFFER) topic.overflow = true;
      else topic.buffer.push(message);
      if (!topic.catching) void heal(topic);
      return;
    }
    emit(topic, message);
  }

  function dispatchPresence(topic: PresenceTopic, event: PresenceEvent): void {
    for (const listener of [...topic.listeners]) {
      try {
        listener(event);
      } catch (error) {
        reportTo(error, topic, "listener");
      }
    }
  }

  function onPresence(topic: PresenceTopic, text: string): void {
    const event = deps.parsePresence(text, topic.channel);
    if (event === undefined || topic.dead) return;
    if (topic.pending !== undefined) {
      // The state is being read: deliver raw now, and fold the event into the state when it arrives.
      if (topic.pending.length < MAX_TRACKED_MEMBERS) topic.pending.push(event);
      else topic.tracked = false;
      dispatchPresence(topic, event);
      return;
    }
    if (topic.tracked && !apply(topic.members, event)) return;
    dispatchPresence(topic, event);
  }

  // --- recovery after a reconnect ------------------------------------------------------------------------------------

  /** Reads history from the cursor until it reaches the present, then lets the held live messages through. */
  async function readToPresent(topic: MessageTopic): Promise<void> {
    for (;;) {
      let more = true;
      while (more) {
        const page = await deps.history(topic.channel, topic.last, CATCH_UP_PAGE);
        if (topic.dead) return;
        for (const message of page) emit(topic, message);
        more = page.length >= CATCH_UP_PAGE;
      }
      if (!topic.overflow) return;
      // More live messages arrived than the buffer holds: read history again from the cursor.
      topic.overflow = false;
      topic.buffer.length = 0;
    }
  }

  /**
   * Fills a gap of the cursor from history. One at a time per topic; a request that arrives while one runs makes it read once
   * more. Gives up after a few failed attempts (the hub's own recovery is the next net) and lets the held messages through.
   */
  async function heal(topic: MessageTopic): Promise<void> {
    if (topic.catching) {
      topic.rerun = true;
      return;
    }
    topic.catching = true;
    try {
      let attempt = 0;
      while (!topic.dead && !closed) {
        topic.rerun = false;
        try {
          await readToPresent(topic);
          if (!topic.rerun) return;
        } catch (error) {
          reportTo(error, topic, "recover");
          const delay = RETRY_DELAYS_MS[attempt];
          attempt += 1;
          if (delay === undefined) return;
          await sleep(delay);
        }
      }
    } finally {
      topic.catching = false;
      topic.rerun = false;
      const held = topic.buffer.splice(0);
      for (const message of held) emit(topic, message);
    }
  }

  async function resync(topic: PresenceTopic): Promise<void> {
    if (!topic.tracked) return;
    const before = topic.members;
    topic.pending = [];
    let list: readonly Member[];
    try {
      list = await deps.members(topic.channel);
    } catch (error) {
      const held = topic.pending;
      topic.pending = undefined;
      for (const event of held) if (apply(before, event)) dispatchPresence(topic, event);
      throw error;
    }
    const held = topic.pending;
    topic.pending = undefined;
    if (topic.dead) return;
    const next = new Map<string, Member>(list.map((member) => [member.id, member]));
    for (const event of held) apply(next, event);
    const events: PresenceEvent[] = [];
    for (const [id, member] of before)
      if (!next.has(id)) events.push({ channel: topic.channel, op: "leave", member });
    for (const [id, member] of next) {
      const known = before.get(id);
      if (known === undefined) events.push({ channel: topic.channel, op: "join", member });
      else if (member.v > known.v) events.push({ channel: topic.channel, op: "set", member });
    }
    topic.members = next;
    if (next.size > MAX_TRACKED_MEMBERS) topic.tracked = false;
    for (const event of events) dispatchPresence(topic, event);
  }

  async function recover(topic: Topic): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        const subscriber = await deps.subscriber();
        await subscriber.subscribe(topic.topic, topic.handler);
        if (topic.kind === "message") {
          if (topic.last > 0) await heal(topic);
        } else {
          await resync(topic);
        }
        return;
      } catch (error) {
        reportTo(error, topic, "reconnect");
        const delay = RETRY_DELAYS_MS[attempt];
        if (delay === undefined || topic.dead || closed) return;
        await sleep(delay);
        if (topic.dead || closed) return;
      }
    }
  }

  async function reconnect(): Promise<void> {
    if (reconnecting) {
      again = true;
      return;
    }
    reconnecting = true;
    try {
      do {
        again = false;
        const live = [...topics.values()].filter((topic) => !topic.dead);
        for (let from = 0; from < live.length && !closed; from += RESUBSCRIBE_BATCH)
          await Promise.all(live.slice(from, from + RESUBSCRIBE_BATCH).map(recover));
      } while (again && !closed);
    } finally {
      reconnecting = false;
    }
  }

  async function hook(): Promise<RedisSubscriber> {
    const subscriber = await deps.subscriber();
    if (connected !== subscriber) {
      connected = subscriber;
      unhook.push(
        subscriber.onReconnect(() => {
          void reconnect();
        }),
      );
      const stopErrors = subscriber.onError?.((error) =>
        deps.report(error, { where: "subscriber" }),
      );
      if (stopErrors !== undefined) unhook.push(stopErrors);
    }
    return subscriber;
  }

  // --- keeping numbering alive ---------------------------------------------------------------------------------------

  async function keepAliveRound(): Promise<void> {
    const channels = [...topics.values()].filter(
      (topic): topic is MessageTopic => topic.kind === "message" && !topic.dead,
    );
    for (let from = 0; from < channels.length; from += RESUBSCRIBE_BATCH)
      await Promise.all(
        channels.slice(from, from + RESUBSCRIBE_BATCH).map(async (topic) => {
          try {
            await deps.keepAlive(topic.channel);
          } catch (error) {
            reportTo(error, topic, "keepAlive");
          }
        }),
      );
  }

  function syncTimer(): void {
    const needed = [...topics.values()].some((topic) => topic.kind === "message");
    if (needed && timer === undefined && !closed) {
      timer = setInterval(() => void keepAliveRound(), deps.keepAliveMs);
      (timer as { unref?: () => void }).unref?.();
    } else if (!needed && timer !== undefined) {
      clearInterval(timer);
      timer = undefined;
    }
  }

  // --- subscribing and releasing -------------------------------------------------------------------------------------

  async function open<T extends Topic>(topic: T): Promise<void> {
    topics.set(topic.topic, topic);
    syncTimer();
    topic.ready = (async () => {
      const subscriber = await hook();
      await subscriber.subscribe(topic.topic, topic.handler);
      if (topic.kind === "presence") await seed(topic);
    })();
    topic.ready.catch(() => {
      // The first caller's await reports it; here the half-open topic is dropped.
      topic.dead = true;
      if (topics.get(topic.topic) === topic) topics.delete(topic.topic);
      syncTimer();
      connected?.unsubscribe(topic.topic).catch(() => undefined);
    });
    await topic.ready;
  }

  async function seed(topic: PresenceTopic): Promise<void> {
    topic.pending = [];
    let list: readonly Member[];
    try {
      list = await deps.members(topic.channel);
    } catch (error) {
      topic.pending = undefined;
      throw error;
    }
    const held = topic.pending;
    topic.pending = undefined;
    const members = new Map<string, Member>(list.map((member) => [member.id, member]));
    for (const event of held) apply(members, event);
    topic.members = members;
    if (members.size > MAX_TRACKED_MEMBERS) topic.tracked = false;
  }

  /**
   * Returns the function that removes one listener. The last listener out drops the topic and sends UNSUBSCRIBE at once (not
   * after an await), so a subscribe of the same topic issued next is sent after it and wins.
   */
  function release(topic: Topic, remove: () => void): Unsubscribe {
    let done = false;
    return async () => {
      if (done) return;
      done = true;
      remove();
      if (topic.listeners.size > 0 || topic.dead) return;
      topic.dead = true;
      if (topics.get(topic.topic) === topic) topics.delete(topic.topic);
      syncTimer();
      if (closed || connected === undefined) return;
      try {
        await connected.unsubscribe(topic.topic);
      } catch (error) {
        reportTo(error, topic, "unsubscribe");
      }
    };
  }

  async function attach<T extends Topic>(
    topic: T | undefined,
    create: () => T,
    add: (topic: T) => Unsubscribe,
  ): Promise<Unsubscribe> {
    if (closed) throw deps.fail(new Error("the adapter is closed."));
    const found = topic ?? create();
    const stop = add(found);
    try {
      if (topic === undefined) await open(found);
      else await found.ready;
    } catch (error) {
      void stop();
      throw deps.fail(error);
    }
    if (closed || found.dead) {
      void stop();
      throw deps.fail(new Error("the adapter is closed."));
    }
    return stop;
  }

  return {
    messages(channel, listener) {
      const key = deps.messageTopic(channel);
      const existing = topics.get(key);
      return attach<MessageTopic>(
        existing?.kind === "message" ? existing : undefined,
        () => {
          const topic: MessageTopic = {
            kind: "message",
            channel,
            topic: key,
            listeners: new Set(),
            last: 0,
            lastAt: 0,
            catching: false,
            rerun: false,
            buffer: [],
            overflow: false,
            dead: false,
            ready: Promise.resolve(),
            handler: (text) => onMessage(topic, text),
          };
          return topic;
        },
        (topic) => {
          topic.listeners.add(listener);
          return release(topic, () => topic.listeners.delete(listener));
        },
      );
    },
    presence(channel, listener) {
      const key = deps.presenceTopic(channel);
      const existing = topics.get(key);
      return attach<PresenceTopic>(
        existing?.kind === "presence" ? existing : undefined,
        () => {
          const topic: PresenceTopic = {
            kind: "presence",
            channel,
            topic: key,
            listeners: new Set(),
            members: new Map(),
            tracked: true,
            pending: undefined,
            dead: false,
            ready: Promise.resolve(),
            handler: (text) => onPresence(topic, text),
          };
          return topic;
        },
        (topic) => {
          topic.listeners.add(listener);
          return release(topic, () => topic.listeners.delete(listener));
        },
      );
    },
    async close() {
      closed = true;
      for (const stop of unhook.splice(0)) stop();
      if (timer !== undefined) clearInterval(timer);
      timer = undefined;
      const open = [...topics.values()];
      topics.clear();
      for (const topic of open) topic.dead = true;
      if (open.length === 0) return;
      const subscriber = connected;
      if (subscriber === undefined) return;
      await Promise.all(
        open.map((topic) =>
          subscriber.unsubscribe(topic.topic).catch((error: unknown) => {
            reportTo(error, topic, "unsubscribe");
          }),
        ),
      );
    },
    get size() {
      return topics.size;
    },
  };
}

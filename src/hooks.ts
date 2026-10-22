import type { Accessor } from "@shadow-js/core";
import { isServer, onCleanup, useSignal } from "@shadow-js/core";
import type {
  AnyMessage,
  Channel,
  ChannelDef,
  ChannelOptions,
  ChannelSchema,
  ChannelStatus,
  MessageMeta,
  PresenceOf,
  Realtime,
  RequestData,
  RequestReply,
  RequestsOf,
  Status,
} from "./client/types";
import { RealtimeError } from "./errors";
import type { Member } from "./protocol";

/**
 * What {@link useChannel} returns: the reactive state of a channel and the calls on it.
 * @since 1.0.0
 */
export interface ChannelHandle<Def extends ChannelDef> {
  /** The channel's status, as a signal. */
  readonly status: Accessor<ChannelStatus>;
  /** The latest message of any event, as a signal (undefined until one arrives). */
  readonly lastMessage: Accessor<AnyMessage<Def> | undefined>;
  /** Publishes a message; see `Channel.publish`. */
  readonly publish: Channel<Def>["publish"];
  /** Sends a request to the server handler; see `Channel.request`. */
  readonly request: <E extends keyof RequestsOf<Def> & string>(
    event: E,
    data: RequestData<Def, E>,
    options?: { readonly timeoutMs?: number },
  ) => Promise<RequestReply<Def, E>>;
  /** The channel itself, for `onGap`, `presence` and the other calls. */
  readonly channel: Channel<Def>;
}

/** A channel for the server render: it never connects, listens or publishes. */
function inertChannel<Def extends ChannelDef>(name: string): Channel<Def> {
  const refuse = (): Promise<never> =>
    Promise.reject(
      new RealtimeError("closed", "a realtime channel is not available during the server render"),
    );
  const none = (): (() => void) => () => undefined;
  return {
    name,
    status: "idle",
    lastSeq: 0,
    on: none,
    onAny: none,
    onGap: none,
    onStatus: none,
    publish: refuse,
    request: refuse,
    presence: { members: () => [], set: () => undefined, leave: () => undefined, on: none },
    close: () => undefined,
  };
}

/**
 * Opens a channel for the owning component and releases it when the component is disposed: `status` and `lastMessage` are
 * signals, `publish` and `request` act on the channel. The channel subscribes while the component lives; other components may
 * share it. During the server render nothing connects.
 *
 * @param rt The client from `createRealtime`.
 * @param name The channel name (fixed for the life of the component; key the component to change it).
 * @param options Echo, presence and replay, applied when the channel is first created.
 * @returns The reactive handle.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 * import { useChannel } from "@shadow-js/realtime/hooks";
 *
 * type Schema = { [name: `chat:${string}`]: { events: { message: { text: string } } } };
 * const rt = createRealtime<Schema>({ url: "/rt" });
 *
 * function Lobby() {
 *   const lobby = useChannel(rt, "chat:lobby");
 *   return <button onClick={() => lobby.publish("message", { text: "hi" })}>{lobby.status()}</button>;
 * }
 * @since 1.0.0
 */
export function useChannel<Schema extends ChannelSchema, Name extends keyof Schema & string>(
  rt: Realtime<Schema>,
  name: Name,
  options?: ChannelOptions<PresenceOf<Schema[Name]>>,
): ChannelHandle<Schema[Name]> {
  const channel: Channel<Schema[Name]> = isServer
    ? inertChannel<Schema[Name]>(name)
    : rt.channel(name, options);
  const [status, setStatus] = useSignal<ChannelStatus>(channel.status);
  const [lastMessage, setLastMessage] = useSignal<AnyMessage<Schema[Name]> | undefined>(undefined);
  if (!isServer) {
    const stopStatus = channel.onStatus((next) => setStatus(next));
    const stopMessages = channel.onAny((message) => setLastMessage(() => message));
    onCleanup(() => {
      stopStatus();
      stopMessages();
    });
  }
  return {
    status,
    lastMessage,
    publish: (event, data, publishOptions) => channel.publish(event, data, publishOptions),
    request: (event, data, requestOptions) => channel.request(event, data, requestOptions),
    channel,
  };
}

/**
 * Calls `handler` for each message of `event` on `channel` while the owning component lives.
 *
 * @param channel A channel from `useChannel(...).channel` or `rt.channel(...)`.
 * @param event The event name.
 * @param handler Called with the data and the message meta.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 * import { useChannel, useChannelEvent } from "@shadow-js/realtime/hooks";
 *
 * type Schema = { [name: `chat:${string}`]: { events: { message: { text: string } } } };
 * const rt = createRealtime<Schema>({ url: "/rt" });
 *
 * function Log() {
 *   const { channel } = useChannel(rt, "chat:lobby");
 *   useChannelEvent(channel, "message", (data) => console.log(data.text));
 *   return <p>listening</p>;
 * }
 * @since 1.0.0
 */
export function useChannelEvent<Def extends ChannelDef, E extends keyof Def["events"] & string>(
  channel: Channel<Def>,
  event: E,
  handler: (data: Def["events"][E], meta: MessageMeta) => void,
): void {
  if (isServer) return;
  onCleanup(channel.on(event, handler));
}

/**
 * The presence set of `channel` as a signal, and a setter that joins it (or changes this client's state) from the owning
 * component. Passing `undefined` leaves. The component leaves the set when it is disposed.
 *
 * @param channel A channel from `useChannel(...).channel` or `rt.channel(...)`.
 * @returns The members and `setPresence`.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 * import { useChannel, useChannelPresence } from "@shadow-js/realtime/hooks";
 *
 * type Schema = {
 *   [name: `chat:${string}`]: { events: { message: string }; presence: { name: string } };
 * };
 * const rt = createRealtime<Schema>({ url: "/rt" });
 *
 * function Who() {
 *   const { channel } = useChannel(rt, "chat:lobby");
 *   const [members, setPresence] = useChannelPresence(channel);
 *   setPresence({ name: "ann" });
 *   return <p>{members().length} here</p>;
 * }
 * @since 1.0.0
 */
export function useChannelPresence<Def extends ChannelDef>(
  channel: Channel<Def>,
): readonly [
  Accessor<readonly Member<PresenceOf<Def>>[]>,
  (state: PresenceOf<Def> | undefined) => void,
] {
  const [members, setMembers] = useSignal<readonly Member<PresenceOf<Def>>[]>(
    channel.presence.members(),
  );
  let joined = false;
  if (!isServer) {
    const stop = channel.presence.on((list) => setMembers(() => list));
    onCleanup(() => {
      stop();
      if (joined) channel.presence.leave();
    });
  }
  return [
    members,
    (state) => {
      if (isServer) return;
      if (state === undefined) {
        joined = false;
        channel.presence.leave();
      } else {
        joined = true;
        channel.presence.set(state);
      }
    },
  ];
}

/**
 * The connection status of `rt` as a signal.
 *
 * @param rt The client from `createRealtime`.
 * @returns The status accessor.
 * @example
 * import { createRealtime } from "@shadow-js/realtime/client";
 * import { useRealtimeStatus } from "@shadow-js/realtime/hooks";
 *
 * const rt = createRealtime({ url: "/rt" });
 *
 * function Badge() {
 *   const status = useRealtimeStatus(rt);
 *   return <span>{status()}</span>;
 * }
 * @since 1.0.0
 */
export function useRealtimeStatus<Schema extends ChannelSchema>(
  rt: Realtime<Schema>,
): Accessor<Status> {
  const [status, setStatus] = useSignal<Status>(rt.status);
  if (!isServer) onCleanup(rt.onStatus((next) => setStatus(next)));
  return status;
}

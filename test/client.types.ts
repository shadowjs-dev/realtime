/**
 * Type tests: compiled by `tsc -p tsconfig.json` (the package typecheck), never executed. Each assignment checks an inferred
 * type; each `@ts-expect-error` checks that misuse is rejected.
 */
import { createRealtime, type Member } from "../src/client/index";
import { useChannel, useChannelEvent, useChannelPresence } from "../src/hooks";

type Schema = {
  [name: `chat:${string}`]: {
    events: { message: { text: string }; typing: { user: string } };
    presence: { name: string };
    requests: { shout: { data: string; reply: string } };
  };
  [name: `ticker:${string}`]: { events: { price: number } };
};

const rt = createRealtime<Schema>({ url: "/rt" });

// [RT-T1] a template-literal key gives every channel of the family its own definition.
const chat = rt.channel("chat:lobby");
chat.on("message", (data, meta) => {
  const text: string = data.text;
  const seq: number = meta.seq;
  void text;
  void seq;
});
rt.channel("ticker:btc").on("price", (price) => price.toFixed(2));

// @ts-expect-error a channel name outside every family
rt.channel("other:1");
// @ts-expect-error an event the channel does not declare
chat.on("nope", () => undefined);
// @ts-expect-error message is an event of chat, not of ticker
rt.channel("ticker:btc").on("message", () => undefined);

// [RT-T2] publish checks the payload against the event.
void chat.publish("message", { text: "hi" });
void chat.publish("typing", { user: "ann" }, { ack: false, key: "k1" });
// @ts-expect-error the text must be a string
void chat.publish("message", { text: 1 });
// @ts-expect-error typing carries a user, not a text
void chat.publish("typing", { text: "x" });
// @ts-expect-error the event is not declared
void chat.publish("nope", 1);
const ack: Promise<{ readonly seq: number; readonly duplicate: boolean }> = chat.publish(
  "message",
  {
    text: "x",
  },
);
void ack;

// [RT-T3] requests carry their own data and reply types.
const reply: Promise<string> = chat.request("shout", "hey");
void reply;
// @ts-expect-error shout takes a string
void chat.request("shout", 1);
// @ts-expect-error no such request
void chat.request("whisper", "x");
// @ts-expect-error ticker declares no requests
void rt.channel("ticker:btc").request("anything", 1);

// [RT-T4] presence state is typed from the definition.
chat.presence.set({ name: "ann" });
// @ts-expect-error the state needs a name
chat.presence.set({ nickname: "ann" });
const members: readonly Member<{ name: string }>[] = chat.presence.members();
void members;
chat.presence.on((list) => {
  const first: { name: string } | undefined = list[0]?.s;
  void first;
});
rt.channel("chat:x", { presence: { name: "ann" } });
// @ts-expect-error the initial presence must match the definition
rt.channel("chat:x", { presence: { name: 1 } });

// [RT-T5] onAny hands a union that narrows on the event.
chat.onAny((message) => {
  if (message.event === "message") {
    const text: string = message.data.text;
    void text;
  } else {
    const user: string = message.data.user;
    void user;
  }
});

// [RT-T6] without a schema every name and event is accepted and data is JSON.
const loose = createRealtime({ url: "/rt" });
void loose.channel("anything:goes").publish("event", { a: [1, 2, { b: null }] });
void loose.channel("anything:goes").request("ask", "x");
// @ts-expect-error a function is not JSON
void loose.channel("anything:goes").publish("event", () => 1);

// [RT-T7] the hooks keep the channel's types.
function component(): void {
  const handle = useChannel(rt, "chat:lobby");
  const last = handle.lastMessage();
  if (last?.event === "message") {
    const text: string = last.data.text;
    void text;
  }
  void handle.publish("message", { text: "x" });
  // @ts-expect-error the payload is checked here too
  void handle.publish("message", { text: 2 });
  const answer: Promise<string> = handle.request("shout", "x");
  void answer;
  useChannelEvent(handle.channel, "typing", (data) => {
    const user: string = data.user;
    void user;
  });
  // @ts-expect-error the event is not declared
  useChannelEvent(handle.channel, "nope", () => undefined);
  const [people, setPresence] = useChannelPresence(handle.channel);
  const name: string | undefined = people()[0]?.s.name;
  void name;
  setPresence({ name: "ann" });
  setPresence(undefined);
  // @ts-expect-error the state needs a name
  setPresence({ nickname: "x" });
  // @ts-expect-error the channel is not in the schema
  useChannel(rt, "other:1");
}
void component;

import { Stream } from "../../stream";

// src/transformers/events.ts
export function events<VALUE>(): Stream.Transformer<Stream<VALUE>, Stream<VALUE> & { events: Stream<events.Event> }> {
  return function (source) {
    const eventsStream = new Stream<events.Event>();

    return Stream.create<VALUE, { events: Stream<events.Event> }>(
      async function* () {
        // Consumer joined (this generator started)
        eventsStream.push({ type: "consumer-joined" });

        if (source.consumersCount === 1) {
          eventsStream.push({ type: "first-consumer-joined" });
          eventsStream.push({ type: "has-one-consumer" });
        }

        try {
          yield* source;
        } finally {
          // Consumer left (this generator ended)
          eventsStream.push({ type: "consumer-leaved" });

          if (source.consumersCount === 0) {
            eventsStream.push({ type: "has-no-consumer" });
          }
        }
      },
      () => ({ events: eventsStream }),
    );
  };
}

export namespace events {
  export type Event =
    | { type: "consumer-joined" }
    | { type: "consumer-leaved" }
    | { type: "first-consumer-joined" }
    | { type: "has-one-consumer" }
    | { type: "has-no-consumer" };

  export type Events = { events: Stream<Event> };
}

import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class EventTargetStream<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends NonEmptyString,
> extends Stream<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME> {
  constructor(
    public readonly target: EventTarget,
    public readonly eventType: EVENT_TYPE,
    options?: EventTargetStream.Options<EVENT_TYPE, NAME>,
  ) {
    super({
      ...options,
      source: {
        listen: (handler, options) => {
          let abortController = new AbortController();
          target.addEventListener(eventType, (e: any) => consumer.push(e), {
            signal: abortController.signal,
          });

          const consumer = new Consumer(handler, {
            ...options,
            events: {
              ...options?.events,
              abort: (self, error) => {
                abortController.abort();
                options?.events?.abort?.(self, error);
              },
              complete: (self) => {
                abortController.abort();
                options?.events?.complete?.(self);
              },
            },
          });

          return consumer;
        },
      },
    });
  }
}

export namespace EventTargetStream {
  export type Options<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}), NAME extends NonEmptyString> = Omit<
    Stream.Options<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME>,
    "source"
  >;
}

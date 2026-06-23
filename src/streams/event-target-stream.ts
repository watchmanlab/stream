import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class EventTargetStream<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends string,
> extends Stream<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME> {
  constructor(
    public readonly target: EventTarget,
    public readonly eventType: EVENT_TYPE,
    init?: Omit<
      stream.Init<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME>,
      "source"
    >,
  ) {
    super({
      ...init,

      source: {
        listen: (init) => {
          let abortController = new AbortController();
          target.addEventListener(eventType, (e: any) => consumer.push(e), {
            signal: abortController.signal,
          });

          const consumer = new Consumer({
            ...init,
            abort: (self, error) => {
              abortController.abort();
              init.abort?.(self, error);
            },
            complete: (self) => {
              abortController.abort();
              init.complete?.(self);
            },
          });

          return consumer;
        },
      },
    });
  }
}

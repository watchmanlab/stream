//@ts-nocheck
import { Stream } from "../core";

const NAME = "event-target-stream";

export class EventTargetStream<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends string = EventTargetStream.Name,
> extends Stream<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME> {
  constructor(target: EventTarget, eventType: EVENT_TYPE, name = NAME as NAME) {
    let abortController: AbortController;

    const queue: Event[] = [];
    let resolve: () => void;

    super(name, async function* () {
      abortController = new AbortController();
      target.addEventListener(
        eventType,
        (e: any) => {
          queue.push(e);
          resolve!?.();
        },
        { signal: abortController.signal },
      );
      try {
        while (true) {
          if (queue.length) {
            yield queue.shift()! as any;
          } else {
            await new Promise<void>((r) => (resolve = r));
          }
        }
      } finally {
        abortController?.abort();
      }
    });
  }
}
export namespace EventTargetStream {
  export type Name = typeof NAME;
}

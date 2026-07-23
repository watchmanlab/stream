import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class EventTargetStream<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends NonEmptyString = "$eventTarget",
> extends Stream<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME> {
  constructor(
    public readonly target: EventTarget,
    public readonly eventType: EVENT_TYPE,
    options?: Stream.Options<
      EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event,
      NAME
    >,
  ) {
    let abortController = new AbortController();

    super({
      ...options,
      name: options?.name ?? ("$eventTarget" as NAME),
      terminate(stream, reason) {
        abortController.abort();
        options?.terminate?.(stream, reason);
      },
    });
    target.addEventListener(eventType, (e: any) => this.push(e), { signal: abortController.signal });
  }
}

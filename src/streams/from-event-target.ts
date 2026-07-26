import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromEventTarget<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends NonEmptyString = "$eventTarget",
> extends Stream<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event, NAME> {
  constructor(
    target: EventTarget,
    eventType: EVENT_TYPE,
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

export function fromEventTarget<
  EVENT_TYPE extends keyof HTMLElementEventMap | (string & {}),
  NAME extends NonEmptyString = "$eventTarget",
>(
  target: EventTarget,
  eventType: EVENT_TYPE,
  options?: Stream.Options<
    EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event,
    NAME
  >,
): FromEventTarget<EVENT_TYPE, NAME> {
  return new FromEventTarget(target, eventType, options);
}

import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class EventTargetSource<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})> extends Source<
  EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event
> {
  constructor(
    private target: EventTarget,
    private eventType: EVENT_TYPE,
  ) {
    super();
  }

  consume(
    handler: Consumer.Handler<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event>,
    options?: Consumer.Options<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event>,
  ): Consumer<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event> {
    const { init, terminate, ...rest } = options ?? {};

    let abortController = new AbortController();

    return new Consumer(handler, {
      ...rest,
      init: (consumer) => {
        this.target.addEventListener(this.eventType, (e: any) => consumer.push(e), { signal: abortController.signal });
        return init?.(consumer);
      },
      terminate(consumer, reason) {
        abortController.abort();
        terminate?.(consumer, reason);
      },
    });
  }
}
export function fromEventTarget<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})>(
  target: EventTarget,
  eventType: EVENT_TYPE,
): EventTargetSource<EVENT_TYPE> {
  return new EventTargetSource(target, eventType);
}

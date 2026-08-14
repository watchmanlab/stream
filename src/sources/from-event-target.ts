import { Consumer } from "../core/consumer";
import { Consumable } from "../core/types";

export function fromEventTarget<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})>(
  target: EventTarget,
  eventType: EVENT_TYPE,
): Consumable<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event> {
  return {
    consume(handler, options) {
      const { init, terminate, ...rest } = options ?? {};

      let abortController = new AbortController();

      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          target.addEventListener(eventType, (e: any) => consumer.push(e), { signal: abortController.signal });
          return init?.(consumer);
        },
        terminate(consumer, reason) {
          abortController.abort();
          terminate?.(consumer, reason);
        },
      });
    },
  };
}

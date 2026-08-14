import { Consumer } from "../core/consumer";
import { Consumable } from "../core/types";

export function fromAbortSignal(signal: AbortSignal): Consumable<void> {
  return {
    consume(handler, options) {
      const { init, terminate, ...rest } = options ?? {};

      let abortController = new AbortController();

      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          if (signal.aborted) {
            consumer.terminate("complete");
          } else {
            signal.addEventListener(
              "abort",
              () => {
                consumer.push();
                consumer.terminate("complete");
              },
              {
                signal: abortController.signal,
              },
            );
          }
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

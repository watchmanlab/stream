import { Consumer } from "../core/consumer";
import { Consumable } from "../core/types";

export function fromAsyncIterator<VALUE>(
  asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
): Consumable<VALUE> {
  return {
    consume(handler, options) {
      const { next, terminate, ...rest } = options ?? {};

      const iter = typeof asyncItrator === "function" ? asyncItrator() : asyncItrator;

      return new Consumer(handler, {
        ...rest,
        next(consumer) {
          iter.next().then((result) => {
            if (result.done) {
              consumer.terminate("complete");
            } else {
              consumer.push(result.value);
            }
          });
          next?.(consumer);
        },
        terminate(consumer, reason) {
          reason === "abort" ? iter.throw?.(reason) : iter.return?.(reason);
          terminate?.(consumer, reason);
        },
      });
    },
  };
}

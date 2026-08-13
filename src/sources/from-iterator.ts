import { Consumer } from "../core/consumer";
import type { Source } from "../core/types";

export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): Source<VALUE> {
  return {
    consume(handler, options) {
      const { next, terminate, ...rest } = options ?? {};

      const iter = typeof iterator === "function" ? iterator() : iterator;

      return new Consumer(handler, {
        ...rest,
        next(consumer) {
          const result = iter.next();

          if (result.done) {
            consumer.terminate("complete");
          } else {
            consumer.push(result.value);
          }
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

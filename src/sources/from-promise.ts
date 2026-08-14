import { Consumer } from "../core/consumer";
import { Result, Consumable } from "../core/types";

export function fromPromise<VALUE>(promise: Promise<VALUE>): Consumable<Result<VALUE>> {
  return {
    consume(handler, options) {
      const { next, init, ...rest } = options ?? {};

      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          promise
            .then((value) => consumer.push({ ok: true, value }))
            .catch((error) => consumer.push({ ok: false, error }))
            .finally(() => consumer.terminate("complete"));
          return init?.(consumer);
        },
      });
    },
  };
}

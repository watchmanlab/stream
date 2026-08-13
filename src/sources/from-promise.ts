import { Consumer } from "../core/consumer";
import { Result, Source } from "../core/types";

export function fromPromise<VALUE>(promise: Promise<VALUE>): Source<Result<VALUE>> {
  return {
    consume(handler, options) {
      const { next, ...rest } = options ?? {};

      return new Consumer(handler, {
        ...rest,
        next(consumer) {
          promise
            .then((value) => consumer.push({ ok: true, value }))
            .catch((error) => consumer.push({ ok: false, error }))
            .finally(() => consumer.terminate("complete"));
          next?.(consumer);
        },
      });
    },
  };
}

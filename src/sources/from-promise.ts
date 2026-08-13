import { Consumer } from "../core/consumer";
import { Result, Source } from "../core/types";

export function fromPromise<VALUE>(promise: Promise<VALUE>): Source<Result<VALUE>> {
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

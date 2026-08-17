import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class PromiseSource<VALUE> extends Source<Result<VALUE>> {
  constructor(private promise: Promise<VALUE>) {
    super();
  }
  consume(
    handler: Consumer.Handler<Result<VALUE>>,
    options?: Consumer.Options<Result<VALUE>>,
  ): Consumer<Result<VALUE>> {
    const { next, init, ...rest } = options ?? {};

    return new Consumer(handler, {
      ...rest,
      init: (consumer) => {
        this.promise
          .then((value) => consumer.push({ ok: true, value }))
          .catch((error) => consumer.push({ ok: false, error }))
          .finally(() => consumer.terminate("complete"));
        return init?.(consumer);
      },
    });
  }
}

export function fromPromise<VALUE>(promise: Promise<VALUE>): PromiseSource<VALUE> {
  return new PromiseSource(promise);
}

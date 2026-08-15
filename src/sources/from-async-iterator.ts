import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class FromAsyncIterator<VALUE> extends Source<VALUE> {
  constructor(private asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>)) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const iter = typeof this.asyncItrator === "function" ? this.asyncItrator() : this.asyncItrator;

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
  }
}

export function fromAsyncIterator<VALUE>(
  asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
): FromAsyncIterator<VALUE> {
  return new FromAsyncIterator(asyncItrator);
}

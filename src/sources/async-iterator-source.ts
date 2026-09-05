import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Wraps an `AsyncIterator` as a pull-based `Source`.
 * Each `next()` call pulls one value asynchronously.
 *
 * @example
 * fromAsyncIterator(asyncIterator).pipe(listen(console.log));
 */
export class AsyncIteratorSource<VALUE> extends Source<VALUE> {
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

/**
 * Creates an `AsyncIteratorSource` from an async iterator or factory.
 * @param asyncItrator An `AsyncIterator` or a factory function returning one.
 */
export function fromAsyncIterator<VALUE>(
  asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
): AsyncIteratorSource<VALUE> {
  return new AsyncIteratorSource(asyncItrator);
}

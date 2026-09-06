import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * `Replayable`
 *
 * Wraps a synchronous `Iterator` as a pull-based `Source`.
 * Each `next()` call pulls one value from the iterator.
 * Terminates with `"complete"` when the iterator is done.
 *
 * @example
 * fromIterator([1, 2, 3].values()).pipe(listen(console.log));
 */
export class IteratorSource<VALUE> extends Source<VALUE> {
  constructor(private iterator: Iterator<VALUE> | (() => Iterator<VALUE>)) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const iter = typeof this.iterator === "function" ? this.iterator() : this.iterator;

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
  }
}
/**
 * `Replayable`
 *
 * Wraps a synchronous `Iterator` as a pull-based `Source`.
 * Each `next()` call pulls one value from the iterator.
 * Terminates with `"complete"` when the iterator is done.
 *
 * @param iterator An `Iterator` or a factory function returning one.
 *
 * @example
 * fromIterator([1, 2, 3].values()).pipe(listen(console.log));
 */
export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): IteratorSource<VALUE> {
  return new IteratorSource(iterator);
}

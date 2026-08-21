import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class IteratorSource<VALUE> extends Source<VALUE> {
  constructor(private iterator: Iterator<VALUE> | (() => Iterator<VALUE>)) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const iter = typeof this.iterator === "function" ? this.iterator() : this.iterator;

    return new Consumer({
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

export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): Consumable<VALUE> {
  return new IteratorSource(iterator);
}

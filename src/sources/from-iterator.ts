import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class FromIterator<VALUE> extends Source<VALUE> {
  constructor(private iterator: Iterator<VALUE> | (() => Iterator<VALUE>)) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
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

export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): Source<VALUE> {
  return new FromIterator(iterator);
}

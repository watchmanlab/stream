import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class IteratorSource<VALUE> extends Source<VALUE> {
  constructor(private iterator: Iterator<VALUE> | (() => Iterator<VALUE>)) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options: Consumer.Options<VALUE> = {}): Consumer<VALUE> {
    const iter = typeof this.iterator === "function" ? this.iterator() : this.iterator;
    const next = options.next;
    const terminate = options.terminate;
    options.next = (consumer) => {
      const result = iter.next();

      if (result.done) {
        consumer.terminate("complete");
      } else {
        consumer.push(result.value);
      }
      next?.(consumer);
    };
    options.terminate = (consumer, reason) => {
      reason === "abort" ? iter.throw?.(reason) : iter.return?.(reason);
      terminate?.(consumer, reason);
    };
    return new Consumer(handler, options);
  }
}

export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): IteratorSource<VALUE> {
  return new IteratorSource(iterator);
}

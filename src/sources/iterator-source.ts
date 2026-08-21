import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class IteratorSource<VALUE> extends Source<VALUE> {
  constructor(private iterator: Iterator<VALUE> | (() => Iterator<VALUE>)) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const iter = typeof this.iterator === "function" ? this.iterator() : this.iterator;

    return new Consumer(new ConsumerOptions(iter, options));
  }
}

export function fromIterator<VALUE>(iterator: Iterator<VALUE> | (() => Iterator<VALUE>)): IteratorSource<VALUE> {
  return new IteratorSource(iterator);
}

class ConsumerOptions<VALUE> extends Consumer.DefaultOptions<VALUE> {
  constructor(
    private iter: Iterator<VALUE>,
    options?: Consumer.Options<VALUE>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<VALUE>): void {
    super.next(consumer);
    const result = this.iter.next();

    if (result.done) {
      consumer.terminate("complete");
    } else {
      consumer.push(result.value);
    }
  }
  override terminate(consumer: Consumer<VALUE>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    reason === "abort" ? this.iter.throw?.(reason) : this.iter.return?.(reason);
  }
}

import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class AsyncIteratorSource<VALUE> extends Source<VALUE> {
  constructor(private asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>)) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const iter = typeof this.asyncItrator === "function" ? this.asyncItrator() : this.asyncItrator;
    return new Consumer(new ConsumerOptions(iter, options));
  }
}

export function fromAsyncIterator<VALUE>(
  asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
): AsyncIteratorSource<VALUE> {
  return new AsyncIteratorSource(asyncItrator);
}

class ConsumerOptions<VALUE> extends Consumer.DefaultOptions<VALUE> {
  constructor(
    private iter: AsyncIterator<VALUE>,
    options?: Consumer.Options<VALUE>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<VALUE>): void {
    super.next(consumer);
    this.iter.next().then((result) => {
      if (result.done) {
        consumer.terminate("complete");
      } else {
        consumer.push(result.value);
      }
    });
  }
  override terminate(consumer: Consumer<VALUE>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    reason === "abort" ? this.iter.throw?.(reason) : this.iter.return?.(reason);
  }
}

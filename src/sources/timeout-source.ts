import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class TimeoutSource<MS extends number> extends Source<void> {
  constructor(private ms: MS) {
    super();
  }

  consume(options?: Consumer.Options<void>): Consumer<void> {
    return new Consumer(new ConsumerOptions(this.ms, options));
  }
}

export function fromTimeout<MS extends number>(ms: MS): TimeoutSource<MS> {
  return new TimeoutSource(ms);
}

class ConsumerOptions extends Consumer.DefaultOptions<void> {
  private timer = null as any;
  constructor(
    private ms: number,
    options?: Consumer.Options<void>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<void>): void {
    super.next(consumer);
    const timer = setTimeout(() => {
      consumer.push();
      consumer.terminate("complete");
    }, this.ms);
  }
  override terminate(consumer: Consumer<void>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    clearTimeout(this.timer);
  }
}

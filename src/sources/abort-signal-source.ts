import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class AbortSignalSource extends Source<void> {
  constructor(private signal: AbortSignal) {
    super();
  }
  consume(options?: Consumer.Options<void>): Consumer<void> {
    let abortController = new AbortController();

    return new Consumer(new ConsumerOptions(this, abortController, options));
  }
}

export function fromAbortSignal(signal: AbortSignal): AbortSignalSource {
  return new AbortSignalSource(signal);
}

class ConsumerOptions extends Consumer.DefaultOptions<void> {
  constructor(
    private abortSignalSource: AbortSignalSource,
    private abortController: AbortController,
    options?: Consumer.Options<void>,
  ) {
    super(options);
  }

  override next(consumer: Consumer<void>): void {
    super.next(consumer);
    if (this.abortSignalSource["signal"].aborted) {
      consumer.terminate("complete");
    } else {
      this.abortSignalSource["signal"].addEventListener(
        "abort",
        () => {
          consumer.push();
          consumer.terminate("complete");
        },
        {
          signal: this.abortController.signal,
        },
      );
    }
  }
  override terminate(consumer: Consumer<void>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    this.abortController.abort();
  }
}

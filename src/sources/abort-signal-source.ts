import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class AbortSignalSource extends Source<void> {
  constructor(private signal: AbortSignal) {
    super();
  }
  consume(handler: Consumer.Handler<void>, options?: Consumer.Options<void>): Consumer<void> {
    const { init, terminate, ...rest } = options ?? {};

    let abortController = new AbortController();

    return new Consumer(handler, {
      ...rest,
      init: (consumer) => {
        if (this.signal.aborted) {
          consumer.terminate("complete");
        } else {
          this.signal.addEventListener(
            "abort",
            () => {
              consumer.push();
              consumer.terminate("complete");
            },
            {
              signal: abortController.signal,
            },
          );
        }
        return init?.(consumer);
      },
      terminate(consumer, reason) {
        abortController.abort();
        terminate?.(consumer, reason);
      },
    });
  }
}

export function fromAbortSignal(signal: AbortSignal): AbortSignalSource {
  return new AbortSignalSource(signal);
}

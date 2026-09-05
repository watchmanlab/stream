import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits once when the given `AbortSignal` is aborted, then completes.
 * If the signal is already aborted at subscription time, emits immediately.
 *
 * @example
 * const controller = new AbortController();
 * fromAbortSignal(controller.signal).pipe(listen(() => console.log('aborted')));
 * controller.abort();
 */
/**
 * Emits once when the given `AbortSignal` is aborted, then completes.
 * If the signal is already aborted at subscription time, emits immediately.
 *
 * @example
 * const controller = new AbortController();
 * fromAbortSignal(controller.signal).pipe(listen(() => console.log('aborted')));
 * controller.abort();
 */
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

/**
 * Creates an `AbortSignalSource`.
 * @param signal The `AbortSignal` to observe.
 */
/**
 * Creates an `AbortSignalSource`.
 * @param signal The `AbortSignal` to observe.
 */
export function fromAbortSignal(signal: AbortSignal): AbortSignalSource {
  return new AbortSignalSource(signal);
}

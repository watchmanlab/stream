import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * `Replayable`
 *
 * Emits a single value after `ms` milliseconds via `setTimeout`, then completes.
 *
 * @example
 * fromTimeout(1000, 'done').pipe(listen(console.log));
 */
export class TimeoutSource<MS extends number, VALUE = void> extends Source<VALUE> {
  constructor(
    private ms: MS,
    private value?: VALUE,
  ) {
    super();
  }

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { init, ...rest } = options ?? {};
    const { ms, value } = this;

    return new Consumer(handler, {
      ...rest,

      init: (consumer) => {
        const timer = setTimeout(() => {
          consumer.push(value as VALUE);
          consumer.terminate("complete");
        }, ms);

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          clearTimeout(timer);
        };
      },
    });
  }
}
/**
 * `Replayable`
 *
 * Emits a single value after `ms` milliseconds via `setTimeout`, then completes.
 *
 * @param ms Delay in milliseconds.
 * @param value Optional value to emit (defaults to `undefined`).
 *
 * @example
 * fromTimeout(1000, 'done').pipe(listen(console.log));
 */
export function fromTimeout<MS extends number, VALUE = void>(ms: MS, value?: VALUE): TimeoutSource<MS, VALUE> {
  return new TimeoutSource(ms, value);
}

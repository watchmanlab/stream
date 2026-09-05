import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits an incrementing counter every `ms` milliseconds via `setInterval`.
 * Never completes on its own — must be terminated externally.
 * `NOTE:` this is a replayable source
 *
 * @example
 * fromInterval(500).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
 */
export class IntervalSource<MS extends number> extends Source<number> {
  constructor(private ms: MS) {
    super();
  }
  consume(handler: Consumer.Handler<number>, options?: Consumer.Options<number>): Consumer<number> {
    const { init, ...rest } = options ?? {};
    let count = 0;
    return new Consumer(handler, {
      ...rest,
      init: (consumer) => {
        const timer = setInterval(() => consumer.push(count++), this.ms);

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          clearInterval(timer);
        };
      },
    });
  }
}

/**
 * Creates an `IntervalSource`.
 * @param ms Interval in milliseconds.
 */
export function fromInterval<MS extends number>(ms: MS): IntervalSource<MS> {
  return new IntervalSource(ms);
}

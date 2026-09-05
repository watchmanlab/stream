import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits integers from `start` (inclusive) to `end` (exclusive), then completes.
 *
 * @example
 * fromRange(1, 5).pipe(listen(console.log)); // 1, 2, 3, 4
 */
export class RangeSource extends Source<number> {
  constructor(
    private start: number,
    private end: number,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    const { next, ...rest } = options ?? {};
    let { start, end } = this;

    return new Consumer(handler, {
      ...rest,
      next(c) {
        const value = start++;
        if (value >= end) c.terminate("complete");
        c.push(value);
        next?.(c);
      },
    });
  }
}

/**
 * Creates a `RangeSource`.
 * @param start First integer to emit (inclusive).
 * @param end Upper bound (exclusive).
 */
export function fromRange(start: number, end: number): RangeSource {
  return new RangeSource(start, end);
}

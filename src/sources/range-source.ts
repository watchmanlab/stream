import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

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

export function fromRange(start: number, end: number): RangeSource {
  return new RangeSource(start, end);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Buffers the last `n` values.
 * When a consumer created, it receives the buffered values then live values.
 * `IMPORTANT`: using latest with a none shared source like "fromIterable" or "fromInterval" and many others require `share`
 * transformer before it , otherwise by consuming it we get the latest values but
 * start the source from the beginning.
 *
 * @example
 * const buffered = fromInterval(300).pipe(share()).pipe(latest(2));
 * setTimeout(() => buffered.pipe(listen(console.log)), 2000);
 */
export class Latest<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  private queue?: DefaultSizedQueue<VALUE>;
  constructor(
    readonly $input: INPUT,
    n: number,
  ) {
    super();

    $input
      .consume(
        (c, v) => {
          (this.queue ??= new DefaultSizedQueue(n)).enqueue(v);
          c.next();
        },
        {
          terminate: () => {
            this.queue?.clear();
            this.queue = undefined;
          },
        },
      )
      .next();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        input$.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        terminate?.(c, r);
      },
    }).pushBatch([...(this.queue ?? [])]);

    const input$ = this.$input.consume((_, v) => output$.push(v), {
      terminate(_, r) {
        output$.terminate(r);
      },
    });
    return output$;
  }
}

/**
 * Buffers the last `n` values.
 * When a consumer created, it receives the buffered values then live values.
 * `IMPORTANT`: using latest with a replayable source like "fromIterable" or "fromInterval" and similare require `share`
 * transformer before it , otherwise by consuming it you get the latest values but
 * replay the source from the beginning.
 *
 * @param n Number of most-recent values to buffer.
 *
 * @example
 * const buffered = fromInterval(300).pipe(share()).pipe(latest(2));
 * setTimeout(() => buffered.pipe(listen(console.log)), 2000);
 */
export function latest<INPUT extends Consumable.AnyConsumable>(n: number) {
  return ($input: INPUT) => new Latest($input, n);
}

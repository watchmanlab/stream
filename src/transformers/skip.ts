import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Skips the first `n` values, then passes all subsequent values through.
 *
 * @example
 * of(1, 2, 3, 4).pipe(skip(2)).pipe(listen(console.log)); // 3, 4
 */
export class Skip<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private n: number,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    let count = this.n;
    return this.$input.consume((c, v) => {
      if (count--) {
        c.next();
      } else {
        c.handler = handler;
        handler(c, v);
      }
    }, options);
  }
}
/**
 * Skips the first `n` values, then passes all subsequent values through.
 *
 *  @param n Number of values to skip.
 *
 * @example
 * of(1, 2, 3, 4).pipe(skip(2)).pipe(listen(console.log)); // 3, 4
 */
export function skip<INPUT extends Consumable.AnyConsumable>(n: number) {
  return ($input: INPUT) => new Skip($input, n);
}

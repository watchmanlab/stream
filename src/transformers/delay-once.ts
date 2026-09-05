import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Delays only the first value by `ms` milliseconds.
 * All subsequent values pass through immediately without delay.
 *
 * @example
 * of(1, 2, 3).pipe(delayOnce(500)).pipe(listen(console.log)); // 1 delayed, 2 and 3 immediate
 */
export class DelayOnce<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private ms: MS,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    let ms = this.ms;
    return this.$input.consume((c, v) => {
      setTimeout(() => {
        c.handler = handler;
        handler(c, v);
      }, ms);
    }, options);
  }
}
/**
 * Delays only the first value by `ms` milliseconds.
 * All subsequent values pass through immediately without delay.
 *
 * @param ms Milliseconds to delay the first value only.
 *
 * @example
 * of(1, 2, 3).pipe(delayOnce(500)).pipe(listen(console.log)); // 1 delayed, 2 and 3 immediate
 */
export function delayOnce<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new DelayOnce($input, ms);
}

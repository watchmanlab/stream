import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Delays each value by `ms` milliseconds using `setTimeout` or `queueMicrotask` if `ms` is less or equal than zero.
 *
 * @example
 * of(1, 2, 3).pipe(delay(500)).pipe(listen(console.log));
 */
export class Delay<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private ms = 0 as MS,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.ms <= 0
      ? this.$input.consume((consumer, value) => queueMicrotask(() => handler(consumer, value)), options)
      : this.$input.consume((consumer, value) => setTimeout(() => handler(consumer, value), this.ms), options);
  }
}
/**
 * Delays each value by `ms` milliseconds using `setTimeout` or `queueMicrotask` if `ms` is less or equal than zero.
 *
 * @param ms Milliseconds to delay each value.
 *
 *  @example
 * of(1, 2, 3).pipe(delay(500)).pipe(listen(console.log));
 */
export function delay<INPUT extends Consumable.AnyConsumable, MS extends number>(ms = 0 as MS) {
  return ($input: INPUT) => new Delay($input, ms);
}

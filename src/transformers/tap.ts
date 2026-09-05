import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Runs a side-effect callback for each value without modifying it.
 *
 * @example
 * of(1, 2, 3).pipe(tap(v => console.log('side effect', v))).pipe(listen());
 */
export class Tap<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private fn: (value: VALUE, index: number) => void,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    let index = 0;
    return this.$input.consume((consumer, value) => (this.fn(value, index++), handler(consumer, value)), options);
  }
}
/**
 * Runs a side-effect callback for each value without modifying it.
 *
 * @param fn `(value, index) => void`
 *
 * @example
 * of(1, 2, 3).pipe(tap(v => console.log('side effect', v))).pipe(listen());
 */
export function tap<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(fn: (value: VALUE, index: number) => void) {
  return ($input: INPUT) => new Tap($input, fn);
}

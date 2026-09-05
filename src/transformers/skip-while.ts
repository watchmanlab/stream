import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Skips values while the predicate returns `true`. Passes all values once it returns `false`.
 *
 * @example
 * of(1, 2, 3, 4).pipe(skipWhile(v => v < 3)).pipe(listen(console.log)); // 3, 4
 */
export class SkipWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private predicate: (value: VALUE) => boolean,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      if (this.predicate(value)) {
        consumer.next();
      } else {
        consumer.handler = handler;
        handler(consumer, value);
      }
    }, options);
  }
}
/**
 * Skips values while the predicate returns `true`. Passes all values once it returns `false`.
 *
 * @param predicate Skip condition.
 *
 * @example
 * of(1, 2, 3, 4).pipe(skipWhile(v => v < 3)).pipe(listen(console.log)); // 3, 4
 */
export function skipWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new SkipWhile($input, predicate);
}

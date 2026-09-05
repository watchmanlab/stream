import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Takes values while the predicate returns `true`. Terminates on the first `false`.
 *
 * @example
 * of(1, 2, 3, 4).pipe(takeWhile(v => v < 3)).pipe(listen(console.log)); // 1, 2
 */
export class TakeWhile<
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
        handler(consumer, value);
      } else {
        consumer.terminate("complete");
      }
    }, options);
  }
}
/**
 * Takes values while the predicate returns `true`. Terminates on the first `false`.
 *
 * @param predicate Stop condition.
 *
 * @example
 * of(1, 2, 3, 4).pipe(takeWhile(v => v < 3)).pipe(listen(console.log)); // 1, 2
 */
export function takeWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new TakeWhile($input, predicate);
}

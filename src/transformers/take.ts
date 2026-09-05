import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Takes the first `n` values then terminates the stream.
 *
 * @example
 * fromInterval(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
 */
export class Take<
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
    return this.$input.consume((consumer, value) => {
      if (count--) {
        handler(consumer, value);
      } else {
        consumer.terminate("complete");
      }
    }, options);
  }
}
/**
 * Takes the first `n` values then terminates the stream.
 *
 * @example
 * fromInterval(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
 */
/** Creates a `take` transformer. @param count Number of values to take. */
export function take<INPUT extends Consumable.AnyConsumable>(n: number) {
  return ($input: INPUT) => new Take($input, n);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits the running count of each value received.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe `last()` transformer after it.
 *
 * @example
 * of('a','b','c').pipe(count()).pipe(last()).pipe(listen(console.log)); // 3
 */
export class Count<INPUT extends Consumable.AnyConsumable> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    let count = 0;

    return this.$input.consume((c, v) => {
      handler(c, ++count);
    }, options);
  }
}

/**
 * Emits the running count of each value received.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe `last()` transformer after it
 *
 * @example
 * of('a','b','c').pipe(count()).pipe(last()).pipe(listen(console.log)); // 3
 */
export function count<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Count($input);
}

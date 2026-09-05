import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits the running maximum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe `last()` transformer after it.
 *
 * @example
 * of(3, 1, 4, 1, 5).pipe(max()).pipe(last()).pipe(listen(console.log)); // 5
 */
export class Max<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    let max = null as number | null;

    return this.$input.consume((c, v) => {
      if (!max) {
        max = v;
      } else {
        max = max < v ? v : max;
      }
      handler(c, max);
    }, options);
  }
}
/**
 * Emits the running maximum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe `last()` transformer after it.
 *
 * @example
 * of(3, 1, 4, 1, 5).pipe(max()).pipe(last()).pipe(listen(console.log)); // 5
 */
export function max<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Max($input);
}

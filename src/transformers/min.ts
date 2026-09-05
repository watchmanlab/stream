import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { last } from "./last";

/**
 * Emits the running minimum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe {@link last} transformer after it.
 *
 * @example
 * of(3, 1, 4, 1, 5).pipe(min()).pipe(last()).pipe(listen(console.log)); // 1
 */
export class Min<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(handler: Consumer.Handler<number>, options?: Consumer.Options<number>): Consumer<number> {
    let min = null as number | null;

    return this.$input.consume((c, v) => {
      if (!min) {
        min = v;
      } else {
        min = min < v ? min : v;
      }
      handler(c, min);
    }, options);
  }
}

/**
 * Emits the running minimum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe {@link last} transformer after it.
 *
 * @example
 * of(3, 1, 4, 1, 5).pipe(min()).pipe(last()).pipe(listen(console.log)); // 1
 */
export function min<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Min($input);
}

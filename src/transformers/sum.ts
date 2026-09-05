import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { last } from "./last";
/**
 * Emits the running sum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe {@link last} transformer after it.
 *
 * @example
 * of(1, 2, 3).pipe(sum()).pipe(last()).pipe(listen(console.log)); // 6
 */
export class Sum<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(handler: Consumer.Handler<number>, options?: Consumer.Options<number>): Consumer<number> {
    let total = 0;

    return this.$input.consume((c, v) => {
      total += v;
      handler(c, total);
    }, options);
  }
}

/**
 * Emits the running sum after each numeric value.
 * all aggregate transformers like this one emit each step,
 * this is useful for tracking intermediate aggregates and can even be paused at any point
 * To get the final count, just pipe {@link last} transformer after it.
 *
 * @example
 * of(1, 2, 3).pipe(sum()).pipe(last()).pipe(listen(console.log)); // 6
 */
export function sum<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Sum($input);
}

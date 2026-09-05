import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Passes only values at indices `[start(0-based), start + offset]`.
 * Values before `start` are skipped; the stream terminates after `offset` values.
 *
 * @example
 * of('a','b','c','d','e').pipe(range(1, 3)).pipe(listen(console.log)); // b, c, d
 */
export class Range<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private start: number,
    private offset: number,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    let index = 0;

    return this.$input.consume((c, v) => {
      if (index++ < this.start) {
        c.next();
      } else if (index <= this.start + this.offset) {
        handler(c, v);
      } else {
        c.terminate("complete");
      }
    }, options);
  }
}
/**
 * Passes only values at indices `[start(0-based), start + offset]`.
 * Values before `start` are skipped; the stream terminates after `offset` values.
 *
 * @param start Index of the first value to pass (0-based).
 * @param offset Number of values to pass.
 *
 * @example
 * of('a','b','c','d','e').pipe(range(1, 3)).pipe(listen(console.log)); // b, c, d
 */
export function range<INPUT extends Consumable.AnyConsumable>(start: number, offset: number) {
  return ($input: INPUT) => new Range($input, start, offset);
}

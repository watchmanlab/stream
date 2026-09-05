import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";
import { last } from "./last";

/**
 * Accumulates values with a reducer and emits the running accumulator after each value.
 * pipe {@link last} after it and you get a standard `reduce` behavior
 *
 * @example
 * of(1, 2, 3)
    .pipe(scan(0, (acc, v) => acc + v))
    .pipe(listen(console.log)); // 1, 3, 6

 * @example  
 * of(1, 2, 3)
    .pipe(scan(0, (acc, v) => acc + v))
    .pipe(last())
    .pipe(listen(console.log)); // 6
 */
export class Scan<
  INPUT extends Consumable.AnyConsumable,
  ACC,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<ACC> {
  constructor(
    private $input: INPUT,
    private acc: ACC,
    private reducer: (acc: ACC, value: VALUE) => ACC,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<ACC>, options?: Consumer.Options<ACC> | undefined): Consumer<ACC> {
    let { acc, reducer } = this;
    return this.$input.consume((c, v) => {
      acc = reducer(acc, v);
      handler(c, acc);
    }, options);
  }
}
/**
 * Accumulates values with a reducer and emits the running accumulator after each value.
 * pipe {@link last} after it and you get a standard `reduce` behavior
 *
 * @param acc Initial accumulator.
 * @param reducer `(acc, value) => acc`.
 *
 * @example 
 * of(1, 2, 3)
    .pipe(scan(0, (acc, v) => acc + v))
    .pipe(listen(console.log)); // 1, 3, 6
 * @example 
 * of(1, 2, 3)
    .pipe(scan(0, (acc, v) => acc + v))
    .pipe(last())
    .pipe(listen(console.log)); // 6
 */
export function scan<
  INPUT extends Consumable.AnyConsumable,
  ACC,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(acc: ACC, reducer: (acc: ACC, value: VALUE) => ACC) {
  return ($input: INPUT) => new Scan($input, acc, reducer);
}

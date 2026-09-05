import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Transforms each value requested by the consumer using a mapper function.
 *
 * @template INPUT The upstream consumable type.
 * @template VALUE The upstream value type.
 * @template MAPPED The output value type.
 *
 * @example
 * of(1, 2, 3).pipe(map(v => v * 2)).pipe(listen(console.log)); // 2, 4, 6
 */
export class Map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  MAPPED = VALUE,
> extends Source<MAPPED> {
  constructor(
    readonly $input: INPUT,
    private mapper: (value: VALUE, index: number) => MAPPED,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>): Consumer<MAPPED> {
    let index = 0;

    return this.$input.consume((c, v) => handler(c, this.mapper(v, index++)), options);
  }
}
/**
 * Transforms each value requested by the consumer using a mapper function.
 *
 * @template INPUT The upstream consumable type.
 * @template VALUE The upstream value type.
 * @template MAPPED The output value type.
 *
 * @param mapper Function `(value, index) => MAPPED` applied to each value.
 *
 * @example
 * of(1, 2, 3).pipe(map(v => v * 2)).pipe(listen(console.log)); // 2, 4, 6
 */
export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  MAPPED = VALUE,
>(mapper: (value: VALUE, index: number) => MAPPED) {
  return ($input: INPUT) => new Map($input, mapper);
}

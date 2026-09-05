import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ValueOfConsumable } from "../core/types";

/**
 * Emits the last value received when the `Consumable` completes.
 * If the `Consumable` completes without emitting, emits `EMPTY`.
 *
 * @example
 * of(1, 2, 3).pipe(last(true)).pipe(listen(console.log)); // 3
 */
export class Last<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  SAFE extends boolean = false,
  OUTPUT_VALUE extends SAFE extends true ? VALUE : VALUE | Empty = SAFE extends true ? VALUE : VALUE | Empty,
> extends Source<OUTPUT_VALUE> {
  constructor(
    private $input: INPUT,
    safe = false as SAFE,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<OUTPUT_VALUE>,
    options?: Consumer.Options<OUTPUT_VALUE> | undefined,
  ): Consumer<OUTPUT_VALUE> {
    const { terminate, ...rest } = options ?? {};
    let last: VALUE | Empty = EMPTY;

    return this.$input.consume(
      (c, v) => {
        last = v;
        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          r === "complete" ? handler(c, last as OUTPUT_VALUE) : handler(c, EMPTY as OUTPUT_VALUE);
          terminate?.(c, r);
        },
      },
    );
  }
}
/**
 * Emits the last value received when the `Consumable` completes.
 * If the `Consumable` completes without emitting, emits `EMPTY`.
 *
 * @param safe Unused, reserved for type narrowing to `VALUE`.
 *
 * @example
 * of(1, 2, 3).pipe(last(true)).pipe(listen(console.log)); // 3
 */
export function last<INPUT extends Consumable.AnyConsumable, SAFE extends boolean = false>(safe = false as SAFE) {
  return ($input: INPUT) => new Last($input, safe);
}

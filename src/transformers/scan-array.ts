import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { last } from "./last";
import { ValueOfConsumable } from "../core/types";

/**
 * Accumulates all values into a growing array and emits the array reference after each push.
 * The same array instance is mutated and emitted each time for performance.
 * Use with {@link last} to get the final collected array.
 *
 * @example
 * of(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
 */
class ScanArray<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE[]> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE[]>,
    options?: Consumer.Options<VALUE[]> | undefined,
  ): Consumer<VALUE[]> {
    const { terminate, ...rest } = options ?? {};
    let array = new Array<VALUE>();

    return this.$input.consume(
      (c, v) => {
        array.push(v);
        handler(c, array);
      },
      {
        ...rest,
        terminate(c, r) {
          (array as any) = null;
          terminate?.(c, r);
        },
      },
    );
  }
}

/**
 * Accumulates all values into a growing array and emits the array reference after each push.
 * The same array instance is mutated and emitted each time for performance.
 * Use with {@link last} to get the final collected array.
 *
 * @example
 * of(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
 */
export function scanArray<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new ScanArray($input);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Emits `true`  when the predicate is true for all values.
 * and `false` when the first predicate check is falsy.
 *
 * @example
 * of(1, 2, 3).pipe(every(v => v > 0)).pipe(last()).pipe(listen(console.log)); // true
 * of(1, -1, 3).pipe(every(v => v > 0)).pipe(last()).pipe(listen(console.log)); // false
 */
export class Every<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<boolean> {
  constructor(
    private $input: INPUT,
    private predicate: (value: VALUE, index: number) => boolean,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<boolean>,
    options?: Consumer.Options<boolean> | undefined,
  ): Consumer<boolean> {
    const { terminate, ...rest } = options ?? {};
    const { predicate } = this;
    let ok = true;
    let index = 0;
    return this.$input.consume(
      (c, v) => {
        if (!predicate(v, index++)) {
          ok = false;
          c.terminate("complete");
        } else {
          c.next();
        }
      },
      {
        ...rest,
        terminate(c, r) {
          handler(c, ok);
          terminate?.(c, r);
        },
      },
    );
  }
}
/**
 * Emits `true`  when the predicate is true for all values.
 * and `false` when the first predicate check is falsy.
 *
 * @param predicate Condition each value must satisfy.
 *
 * @example
 * of(1, 2, 3).pipe(every(v => v > 0)).pipe(last()).pipe(listen(console.log)); // true
 * of(1, -1, 3).pipe(every(v => v > 0)).pipe(last()).pipe(listen(console.log)); // false
 */
export function every<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(predicate: (value: VALUE, index: number) => boolean) {
  return ($input: INPUT) => new Every($input, predicate);
}

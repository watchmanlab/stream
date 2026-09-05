import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ValueOfConsumable } from "../core/types";

/**
 * Emits the first value received then terminates.
 * Or emits the value when the {@link predicate} return `true` then terminates.
 * Or emit EMPTY if the source is empty or all {@link predicate} calls return `false`.
 *
 * @example
 * of(1, 2, 3).pipe(first()).pipe(listen(console.log)); // 1
 */
export class First<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE | Empty> {
  constructor(
    private $input: INPUT,
    private predicate?: (value: VALUE, index: number) => boolean,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE | Empty>,
    options?: Consumer.Options<VALUE | Empty> | undefined,
  ): Consumer<VALUE | Empty> {
    const { terminate, ...rest } = options ?? {};
    const { predicate } = this;

    let value: VALUE | Empty = EMPTY;
    let index = 0;

    return this.$input.consume(
      !predicate
        ? (c, v) => {
            value = v;
            c.terminate("complete");
          }
        : (c, v) => {
            if (predicate(v, index++)) {
              value = v;
              c.terminate("complete");
            } else {
              c.next();
            }
          },
      {
        ...rest,
        terminate(c, r) {
          handler(c, value);
          terminate?.(c, r);
        },
      },
    );
  }
}

/**
 * Emits the first value received then terminates.
 * Or emits the value when the {@link predicate} return `true` then terminates.
 * Or emit EMPTY if the source is empty or all {@link predicate} calls return `false`.
 *
 * @param predicate
 *
 * @example
 * of(1, 2, 3).pipe(first()).pipe(listen(console.log)); // 1
 */
export function first<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(predicate?: (value: VALUE, index: number) => boolean) {
  return ($input: INPUT) => new First($input, predicate);
}

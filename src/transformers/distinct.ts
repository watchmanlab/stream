import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Filters out duplicate values using a `Set`.
 * Optionally accepts a `keySelector` to compare by a derived key,
 * and a `$flushes` notifier to reset the seen-set.
 *
 * @example
 * of(1, 1, 2, 2, 3).pipe(distinct()).pipe(listen(console.log)); // 1, 2, 3
 * of({id:1},{id:1},{id:2}).pipe(distinct(v => v.id)).pipe(listen(console.log));
 */
export class Distinct<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  KEY = VALUE,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private keySelector?: (value: VALUE) => KEY,
    private $flushes?: Consumable.AnyConsumable,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    const { keySelector } = this;
    const valuesOrKeys = new Set<VALUE | KEY>();

    const input$ = this.$input.consume(
      keySelector
        ? (c, v) => {
            const key = keySelector(v);
            if (valuesOrKeys.has(key)) {
              c.next();
            } else {
              valuesOrKeys.add(key);
              handler(c, v);
            }
          }
        : (c, v) => {
            if (valuesOrKeys.has(v)) {
              c.next();
            } else {
              valuesOrKeys.add(v);
              handler(c, v);
            }
          },
      {
        ...rest,
        terminate(c, r) {
          valuesOrKeys.clear();
          flushes$?.terminate(r);
          terminate?.(c, r);
        },
      },
    );

    const flushes$ = this.$flushes
      ?.consume(
        (c) => {
          valuesOrKeys.clear();
          c.next();
        },
        {
          terminate(c, r) {
            input$.terminate(r);
          },
        },
      )
      .next();
    return input$;
  }
}
/**
 * Filters out duplicate values using a `Set`.
 * Optionally accepts a `keySelector` to compare by a derived key,
 * and a `$flushes` notifier to reset the seen-set.
 *
 * @param keySelector Optional function to derive the comparison key.
 * @param $flushes Optional notifier that resets the seen-set when it emits.
 *
 * @example
 * of(1, 1, 2, 2, 3).pipe(distinct()).pipe(listen(console.log)); // 1, 2, 3
 * of({id:1},{id:1},{id:2}).pipe(distinct(v => v.id)).pipe(listen(console.log));
 */

export function distinct<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  KEY = VALUE,
>(keySelector?: (value: VALUE) => KEY, $flushes?: Consumable.AnyConsumable) {
  return ($input: INPUT) => new Distinct($input, keySelector, $flushes);
}

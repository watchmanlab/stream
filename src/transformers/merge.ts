import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { ValueOfConsumable } from "../core/types";

/**
 * Merges multiple consumables into one, emitting values from all inputs as they arrive.
 * Terminates when the primary input (`$input`) terminates.
 *
 * @example
 * const s1 = fromInterval(500).pipe(map(() => 'A'));
 * const s2 = fromInterval(700).pipe(map(() => 'B'));
 * s1.pipe(merge(s2)).pipe(listen(console.log));
 */
export class Merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
  VALUE extends ValueOfConsumable<INPUT> | ValueOfConsumable<OTHERS[number]> =
    | ValueOfConsumable<INPUT>
    | ValueOfConsumable<OTHERS[number]>,
> extends Source<VALUE> {
  private $inputs: [INPUT, ...OTHERS];
  constructor(
    readonly $input: INPUT,
    $others: OTHERS,
  ) {
    super();

    this.$inputs = [$input, ...$others];
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        consumers.forEach((entry) => {
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer.next();
          }
        });

        next?.(c);
      },
      terminate(c, r) {
        consumers.forEach((entry) => entry.consumer.terminate(r));
        consumers.length = 0;
        terminate?.(c, r);
      },
    });

    const consumers = this.$inputs.map(($input, index) => {
      const entry = {
        consumer: $input.consume(
          (_, v) => {
            entry.pending = false;
            output$.push(v);
          },
          index === 0
            ? {
                terminate(_, r) {
                  output$.terminate(r);
                },
              }
            : undefined,
        ),
        pending: false,
      };
      return entry;
    });

    return output$;
  }
}

/**
 * Merges multiple consumables into one, emitting values from all inputs as they arrive.
 * Terminates when the primary input (`$input`) terminates.
 *
 * @param others Additional streams to merge with the input.
 *
 * @example
 * const s1 = fromInterval(500).pipe(map(() => 'A'));
 * const s2 = fromInterval(700).pipe(map(() => 'B'));
 * s1.pipe(merge(s2)).pipe(listen(console.log));
 */
export function merge<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Merge($input, others);
}

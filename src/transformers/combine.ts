import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ZipedArray } from "../core/types";

/**
 * Emits the value or {@link EMPTY} from each input whenever any input emits.
 * To get the CombineLatest behvior, use `zip` and pipe `latest` to each input .
 * Unlike `zip`, does not wait for all inputs to have a value — uses the last known value or {@link EMPTY} .
 *
 * @example
 * const s1 = of(1,2,3).pipe(delay(100));
 * const s2 = of('a','b').pipe(delay(200));
 * s1.pipe(combine(s2)).pipe(listen(console.log));
 */
export class Combine<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
  VALUE extends ZipedArray<[INPUT, ...OTHERS]> = ZipedArray<[INPUT, ...OTHERS]>,
> extends Source<VALUE> {
  private $inputs: [INPUT, ...OTHERS];

  constructor($input: INPUT, $others: OTHERS) {
    super();
    this.$inputs = [$input, ...$others];
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.$inputs.length).fill(EMPTY);

    const output$ = new Consumer<VALUE>(handler, {
      ...rest,
      next(r) {
        consumers.forEach((entry) => {
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer$.next();
          }
        });
        next?.(r);
      },
      terminate: (c, r) => {
        consumers.forEach((entry) => entry.consumer$.terminate(r));
        consumers.length = 0;
        buffer.length = 0;
        terminate?.(c, r);
      },
    });

    const consumers = this.$inputs.map(($input, index) => {
      const entry = {
        consumer$: $input.consume(
          (_, value) => {
            buffer[index] = value;
            entry.pending = false;
            output$.push([...buffer] as any);
          },
          {
            terminate(c, r) {
              output$.terminate(r);
            },
          },
        ),
        pending: false,
      };
      return entry;
    });
    return output$;
  }
}

/**
 * Emits the value or {@link EMPTY} from each input whenever any input emits.
 * To get the CombineLatest behvior, we need to pipe latests to each input.
 * Unlike `zip`, does not wait for all inputs to have a value — uses the last known value or {@link EMPTY} .
 *
 * @param others Additional streams to combine with the input.
 *
 * @example
 * const s1 = of(1,2,3).pipe(delay(100));
 * const s2 = of('a','b').pipe(delay(200));
 * s1.pipe(combine(s2)).pipe(listen(console.log));
 */
export function combine<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Combine($input, others);
}

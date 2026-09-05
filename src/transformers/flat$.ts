import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { isConsumable, ValueOfConsumable } from "../core/types";

/**
 * Flattens a `Consumable` of `Consumable`s sequentially — consumes each inner `Consumable`
 * and waits for it to complete before moving to the next.
 *
 * @example
 * of(of(1,2), of(3,4)).pipe(flat$()).pipe(listen(console.log)); // 1,2,3,4
 */
export class Flat$<
  INPUT extends Consumable.AnyConsumable,
  DEPTH extends number = 1,
  VALUE extends ValueOfConsumable<INPUT, DEPTH> = ValueOfConsumable<INPUT, DEPTH>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    depth = 1 as DEPTH,
  ) {
    super();

    let flat$: Flat$<any, any, any> = this;

    while (depth-- > 1) {
      flat$ = new Flat$(flat$);
    }

    return flat$;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        v$.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        v$.terminate(r);
        terminate?.(c, r);
      },
    });

    const input$ = this.$input.consume(
      (_, v) => {
        if (isConsumable<VALUE>(v)) {
          v$ = v.consume(
            (_, v) => {
              output$.push(v);
            },
            {
              terminate: () => {
                v$ = input$;
                input$.next();
              },
            },
          );
          v$.next();
        } else {
          output$.push(v);
        }
      },
      { terminate: (_, r) => output$.terminate(r) },
    );
    let v$: Consumer<any> = input$;

    return output$;
  }
}

/**
 * Flattens a `Consumable` of `Consumable`s sequentially — consumes each inner `Consumable`
 * and waits for it to complete before moving to the next.
 *
 * @param depth How many levels of nesting to flatten sequentially (default `1`).
 *
 * @example
 * of(of(1,2), of(3,4)).pipe(flat$()).pipe(listen(console.log)); // 1,2,3,4
 */

export function flat$<INPUT extends Consumable.AnyConsumable, DEPTH extends number = 1>(depth = 1 as DEPTH) {
  return ($input: INPUT) => new Flat$($input, depth);
}

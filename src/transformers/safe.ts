import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable, Error } from "../core/types";

/**
 * Wraps a transformer pipeline in a try/catch.
 * On success passes values through; on thrown error emits `Error<thrown>`.
 *
 * @example
 * of(1,2,3).pipe(safe(map(v => { if(v===2) throw 'bad'; return v; }))).pipe(listen(console.log));
 */
export class Safe<
  INPUT extends Consumable.AnyConsumable,
  OUTPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<OUTPUT> = ValueOfConsumable<OUTPUT>,
> extends Source<VALUE | Error<any>> {
  constructor(
    private $input: INPUT,
    private fn: ($input: INPUT) => OUTPUT,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE | Error<any>>,
    options?: Consumer.Options<VALUE | Error<any>> | undefined,
  ): Consumer<VALUE | Error<any>> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        try {
          input$.next();
          next?.(c);
        } catch (error) {
          c.push(new Error(error));
        }
      },
      terminate(c, r) {
        input$.terminate(r);
        terminate?.(c, r);
      },
    });

    const input$ = this.fn(this.$input).consume((_, v) => output$.push(v), {
      terminate: (_, r) => output$.terminate(r),
    });

    return output$;
  }
}

/**
 * Wraps a transformer pipeline in a try/catch.
 * On success passes values through; on thrown error emits `Error<thrown>`.
 *
 * @param fn A function that takes the input and returns a transformed consumable.
 *
 * @example
 * of(1,2,3).pipe(safe(map(v => { if(v===2) throw 'bad'; return v; }))).pipe(listen(console.log));
 */
export function safe<INPUT extends Consumable.AnyConsumable, OUTPUT extends Consumable.AnyConsumable>(
  fn: ($input: INPUT) => OUTPUT,
) {
  return ($input: INPUT) => new Safe($input, fn);
}

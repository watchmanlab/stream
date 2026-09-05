import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Takes values until the notifier emits, then terminates.
 *
 * @example
 * const stop = fromTimeout(1000);
 * fromInterval(200).pipe(takeUntil(stop)).pipe(listen(console.log));
 */
export class TakeUntil<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private $notifier: Consumable.AnyConsumable,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const output$ = this.$input.consume(handler, {
      ...rest,
      terminate(c, r) {
        notifier$.terminate(r);
        terminate?.(c, r);
      },
    });

    const notifier$ = this.$notifier
      .consume(
        (c) => {
          output$.terminate("complete");
          c.terminate("complete");
        },
        {
          terminate(_, r) {
            output$.terminate(r);
          },
        },
      )
      .next();

    return output$;
  }
}
/**
 * Takes values until the notifier emits, then terminates.
 *
 * @param $notifier Terminates when this emits.
 *
 * @example
 * const stop = fromTimeout(1000);
 * fromInterval(200).pipe(takeUntil(stop)).pipe(listen(console.log));
 */
export function takeUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new TakeUntil($input, $notifier);
}

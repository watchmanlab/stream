import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Skips all values until the notifier emits, then passes all subsequent values through.
 *
 * @example
 * const start = fromTimeout(600);
 * stream.pipe(skipUntil(start)).pipe(listen(console.log));
 */
export class SkipUntil<
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

    let skip = true;
    const notifier$ = this.$notifier
      .consume((c) => {
        skip = false;
        c.terminate("complete");
      })
      .next();

    return this.$input.consume(
      (c, v) => {
        if (skip) {
          c.next();
        } else {
          c.handler = handler;
          handler(c, v);
        }
      },
      {
        ...rest,
        terminate(c, r) {
          notifier$.terminate(r);
          terminate?.(c, r);
        },
      },
    );
  }
}
/**
 * Skips all values until the notifier emits, then passes all subsequent values through.
 *
 * @param $notifier Start passing values when this emits.
 *
 * @example
 * const start = fromTimeout(600);
 * stream.pipe(skipUntil(start)).pipe(listen(console.log));
 */
export function skipUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new SkipUntil($input, $notifier);
}

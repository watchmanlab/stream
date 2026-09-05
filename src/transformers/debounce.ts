import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Emits the latest value only after `ms` milliseconds of silence .
 * Each new incoming value resets the timer.
 *
 * @example
 * stream.pipe(debounce(300)).pipe(listen(console.log));
 */
export class Debounce<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private ms: MS,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const { ms } = this;

    let timer = null as any;

    return this.$input.consume(
      (c, v) => {
        clearTimeout(timer);

        timer = setTimeout(() => {
          handler(c, v);
        }, ms);

        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          clearTimeout(timer);
          terminate?.(c, r);
        },
      },
    );
  }
}

/**
 * Emits the latest value only after `ms` milliseconds of silence .
 * Each new incoming value resets the timer.
 *
 * @param ms Silence window in milliseconds.
 *
 * @example
 * stream.pipe(debounce(300)).pipe(listen(console.log));
 */
export function debounce<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Debounce($input, ms);
}

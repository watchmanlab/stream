import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Rate-limits output to at most one value per `ms` milliseconds.
 * If a value arrives before the window expires, it is scheduled for the next slot.
 * If the value arrives after the window expires, it is emitted directly
 * Credit flows backwards to slow the upstream producer.
 *
 * @example
 * fromInterval(50).pipe(pace(500)).pipe(listen(console.log));
 */
export class Pace<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private ms: MS,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    const { ms, $input } = this;

    let timer: any = null;
    let nextAllowedExecutionTime = 0;

    return $input.consume(
      (c, v) => {
        const now = performance.now();

        if (now >= nextAllowedExecutionTime) {
          nextAllowedExecutionTime = now + ms;
          handler(c, v);
        } else {
          const delayRemainder = nextAllowedExecutionTime - now;
          nextAllowedExecutionTime += ms;

          timer = setTimeout(() => {
            handler(c, v);
          }, delayRemainder);
        }
      },
      {
        ...rest,
        terminate(c, r) {
          terminate?.(c, r);
          clearTimeout(timer);
        },
      },
    );
  }
}
/**
 * Rate-limits output to at most one value per `ms` milliseconds.
 * If a value arrives before the window expires, it is scheduled for the next slot.
 * If the value arrives after the window expires, it is emitted directly
 * Credit flows backwards to slow the upstream producer.
 *
 * @param ms Minimum interval in milliseconds between consumed values.
 *
 * @example
 * fromInterval(50).pipe(pace(500)).pipe(listen(console.log));
 */
export function pace<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Pace($input, ms);
}

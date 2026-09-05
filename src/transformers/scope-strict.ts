import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";
import { passive } from "./passive";
import { Scope, scope } from "./scope";

/**
 * Like {@link Scope} but requires ALL notifiers to complete before terminating.
 *
 * @example
 * const s1 = fromTimeout(1000);
 * const s2 = fromTimeout(2000);
 * fromInterval(300).pipe(scopeStrict(s1, s2)).pipe(listen(console.log)); // runs until both fire
 */
export class ScopeStrict<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private others: OTHERS,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    let count = this.others.length;

    const output$ = this.$input.consume(handler, {
      ...rest,
      terminate(c, r) {
        others$.forEach((c) => c.terminate(r));
        terminate?.(c, r);
      },
    });
    const others$ = this.others.map((other) =>
      Source.from(other)
        .pipe(passive())
        .consume((c) => c.next(), {
          terminate: (_, r) => !--count && output$.terminate(r),
        })
        .next(),
    );

    return output$;
  }
}
/**
 * Like {@link scope} but requires ALL notifiers to complete before terminating.
 *
 * @param others All notifiers must complete before the output terminates.
 *
 * @example
 * const s1 = fromTimeout(1000);
 * const s2 = fromTimeout(2000);
 * fromInterval(300).pipe(scopeStrict(s1, s2)).pipe(listen(console.log)); // runs until both fire
 */
export function scopeStrict<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [other: Consumable.AnyConsumable, ...others: Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new ScopeStrict($input, others);
}

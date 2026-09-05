import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { isConsumable, ValueOfConsumable } from "../core/types";
import { of } from "../sources/of-source";
/**
 * Consume the latest inner `Consumable`, cancelling the previous one when a new one arrives.
 * Non-consumable values are passed through directly.
 * `NOTE`: values(consumables) from replayable input sources like {@link of} will be ignored,
 * since this transformer is eager on the input values but lazy on the inner consumable,
 * so the source will teminate before attaching a consumer to the last value(consumable)
 *
 * @example
 *  const inner1 = of(1, 2);
    const inner2 = of("a", "b");
    const stream = new Stream<number|Consumable<number>|Consumable<string>>();

    stream.pipe(switch$()).pipe(listen((v) => results.push(v)));//1, 2, 3, "a", "b"

    stream.push(inner1);
    stream.push(3);
    stream.push(inner2);
 */
export class Switch$<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT, 1> = ValueOfConsumable<INPUT, 1>,
> extends Source<VALUE> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { init, next, terminate, ...rest } = options ?? {};
    const { $input } = this;
    let current$: Consumer<VALUE> | null = null;

    const output$ = new Consumer(handler, {
      ...rest,
      init(output$) {
        const input$ = $input
          .consume(
            (c, v) => {
              current$?.terminate("abort");
              current$ = null;

              if (!isConsumable<VALUE>(v)) {
                if (output$.credit) output$.push(v);
                c.next();
                return;
              }

              current$ = v.consume((_, v) => output$.push(v));
              if (output$.credit) current$.next();
              c.next();
            },
            {
              terminate(c, r) {
                output$.terminate(r);
              },
            },
          )
          .next();

        const initCleanup = init?.(output$);
        return (r) => {
          input$.terminate(r);
          initCleanup?.(r);
        };
      },
      next(c) {
        current$?.next();
        next?.(c);
      },
      terminate(c, r) {
        current$?.terminate(r);
        terminate?.(c, r);
      },
    });

    return output$;
  }
}

/**
 * Consume the latest inner `Consumable`, cancelling the previous one when a new one arrives.
 * Non-consumable values are passed through directly.
 * `NOTE`: values(consumables) from replayable input sources like {@link of} will be ignored,
 * since this transformer is eager on the input values but lazy on the inner consumable,
 * so the source will teminate before attaching a consumer to the last value(consumable)
 *
 * @example
 *  const inner1 = of(1, 2);
    const inner2 = of("a", "b");
    const stream = new Stream<number|Consumable<number>|Consumable<string>>();

    stream.pipe(switch$()).pipe(listen((v) => results.push(v)));//1, 2, 3, "a", "b"

    stream.push(inner1);
    stream.push(3);
    stream.push(inner2);
 */
export function switch$<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Switch$($input);
}

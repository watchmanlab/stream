import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Opens or closes the stream based on a boolean control `Consumable`.
 * `true` opens the gate (consume the $input), `false` closes it (terminates inner consumer).
 *
 * @example
 * const toggle = fromInterval(3000).pipe(map((_, i) => i % 2 === 0));
 * fromInterval(500).pipe(gate(toggle)).pipe(listen(console.log));
 */
export class Gate<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private $control: Consumable<boolean>,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    let input$: Consumer<VALUE> | undefined;

    const output$ = new Consumer<VALUE>(handler, {
      ...rest,
      next(c) {
        input$?.next();
        next?.(c);
      },
      terminate(c, r) {
        input$?.terminate(r);
        control$.terminate(r);
        terminate?.(c, r);
      },
    });

    const control$ = this.$control
      .consume(
        (c, v) => {
          if (v) {
            input$ = this.$input.consume((_, v) => output$.push(v)).next();
          } else {
            input$?.terminate("complete");
            input$ = undefined;
          }
          c.next();
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
 * Opens or closes the stream based on a boolean control `Consumable`.
 * `true` opens the gate (consume the $input), `false` closes it (terminates inner consumer).
 *
 * @param control A stream of booleans. `true` opens, `false` closes.
 *
 * @example
 * const toggle = fromInterval(3000).pipe(map((_, i) => i % 2 === 0));
 * fromInterval(500).pipe(gate(toggle)).pipe(listen(console.log));
 */
export function gate<INPUT extends Consumable.AnyConsumable>(control: Consumable<boolean>) {
  return ($input: INPUT) => new Gate($input, control);
}

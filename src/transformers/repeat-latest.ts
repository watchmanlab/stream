import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ValueOfConsumable } from "../core/types";

export class Repeat<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};
    let value: VALUE | Empty = EMPTY;

    const input$ = this.$input
      .consume((c, v) => {
        value = v;
        c.next();
      })
      .next();

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        if (value !== EMPTY) {
          c.push(value);
        } else {
          input$.$handle
            .consume((hc, v) => {
              c.push(v);
              hc.terminate("complete");
            })
            .next();
        }
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        value = EMPTY;
        terminate?.(c, r);
      },
    });

    return output$;
  }
}

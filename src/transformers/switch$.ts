import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Switch$<
  INPUT extends Consumable<Consumable.AnyConsumable>,
  VALUE extends ValueOfConsumable<INPUT, 1> = ValueOfConsumable<INPUT, 1>,
> extends Source<VALUE> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    let current$: Consumer<VALUE> | null = null;

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        if (!current$) input$.next();
        else current$?.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        current$?.terminate(r);
        terminate?.(c, r);
      },
    });

    const input$ = this.$input.consume((c, v) => {
      current$?.terminate("abort");

      c.next();
      current$ = v.consume((c, v) => output$.push(v)).next();
    });

    return output$;
  }
}

export function switch$<INPUT extends Consumable<Consumable.AnyConsumable>>() {
  return ($input: INPUT) => new Switch$($input);
}

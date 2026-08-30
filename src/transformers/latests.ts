import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Latests<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private count: number,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { count } = this;
    const { next, terminate, ...rest } = options ?? {};

    const input$ = this.$input.consume(
      (c, v) => {
        output$.push(v);
      },
      {
        queueFactory: () => new DefaultSizedQueue(count),
        terminate(c, r) {
          output$.terminate(r);
        },
      },
    );

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        input$.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        terminate?.(c, r);
      },
    });

    return output$;
  }
}

export function latests<INPUT extends Consumable.AnyConsumable>(count: number) {
  return ($input: INPUT) => new Latests($input, count);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Replay<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private values: [VALUE, ...VALUE[]],
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const input$ = this.$input.consume((c, v) => output$.push(v), {
      terminate(consumer, reason) {
        output$.terminate(reason);
      },
    });

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        input$.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        input$.terminate(reason);
        terminate?.(consumer, reason);
      },
    }).pushBatch(this.values);

    return output$;
  }
}

export function replay<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(values: [VALUE, ...VALUE[]]) {
  return ($input: INPUT) => new Replay($input, values);
}

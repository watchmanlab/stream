import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class TakeUntil<
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

    const output$ = this.$input.consume(handler, {
      ...rest,
      terminate(consumer, reason) {
        notifier$.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const notifier$ = this.$notifier
      .consume((consumer) => {
        output$.terminate("complete");
        consumer.terminate("complete");
      })
      .next();

    return output$;
  }
}

export function takeUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new TakeUntil($input, $notifier);
}

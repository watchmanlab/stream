import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Transformer } from "../core/types";

export class TakeUntil<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private $notifier: Consumable.AnyConsumable,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const inputConsumer = this.$input.consume(handler, {
      ...rest,
      terminate(consumer, reason) {
        notifierConsumer.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const notifierConsumer = this.$notifier
      .consume((consumer) => {
        inputConsumer.terminate("complete");
        consumer.terminate("complete");
      })
      .next();

    return inputConsumer;
  }
}

export function takeUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new TakeUntil($input, $notifier);
}

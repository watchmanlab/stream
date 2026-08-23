import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class SkipUntil<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private $notifier: AnyConsumable,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    let skip = true;
    const notifierConsumer = this.$notifier
      .consume((consumer) => {
        skip = false;
        consumer.terminate("complete");
      })
      .next();

    return this.$input.consume(
      (consumer, value) => {
        if (skip) {
          consumer.next();
        } else {
          consumer["_handler"] = handler;
          handler(consumer, value);
        }
      },
      {
        ...rest,
        terminate(consumer, reason) {
          notifierConsumer.terminate(reason);
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function skipUntil<INPUT extends AnyConsumable>($notifier: AnyConsumable) {
  return ($input: INPUT) => new SkipUntil($input, $notifier);
}

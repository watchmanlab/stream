import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class Passive<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(readonly $input: INPUT) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const outputConsumer = new Consumer(handler, {
      ...rest,
      terminate(consumer, reason) {
        inputConsumer.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const inputConsumer = this.$input.consume((_, value) => outputConsumer.push(value), {
      push(_, value) {
        outputConsumer.push(value);
      },
      terminate(_, reason) {
        outputConsumer.terminate(reason);
      },
    });

    return outputConsumer;
  }
}

export function passive<INPUT extends AnyConsumable>() {
  return ($input: INPUT) => new Passive($input);
}

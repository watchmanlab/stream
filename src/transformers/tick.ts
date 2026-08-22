import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Tick<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(readonly $input: INPUT) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume(new ConsumerOptions(options));
  }
}

export function tick<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Tick($input);
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(options?: Consumer.Options<any>) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void | undefined {
    queueMicrotask(() => super.handler(consumer, value));
  }
}

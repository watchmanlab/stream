import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class TakeWhile<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private predicate: (value: VALUE) => boolean,
  ) {
    super();
  }

  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume(new ConsumerOptions(this.predicate, options));
  }
}

export function takeWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new TakeWhile($input, predicate);
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private predicate: (value: any) => boolean,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void {
    if (this.predicate(value)) {
      super.handler(consumer, value);
    } else {
      consumer.terminate("complete");
    }
  }
}

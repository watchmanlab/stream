import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Skip<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private count: number,
  ) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume(new ConsumerOptions(this.count, options));
  }
}

export function skip<INPUT extends Consumable.AnyConsumable>(count: number) {
  return ($input: INPUT) => new Skip($input, count);
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private count: number,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void {
    if (this.count > 0) {
      this.count--;
      consumer.next();
    } else {
      this.options?.handler?.(consumer, value);
    }
  }
}

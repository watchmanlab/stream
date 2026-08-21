import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Tap<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private callback: (value: VALUE) => void,
  ) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume(new ConsumerOptions(this.callback, options));
  }
}
export function tap<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  callback: (value: VALUE) => void,
) {
  return ($input: INPUT) => new Tap($input, callback);
}
class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private callback: (value: any) => void,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void {
    this.callback(value);
    super.handler(consumer, value);
  }
}

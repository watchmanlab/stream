import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class TapBatch<
  INPUT extends Consumable<Array<any>>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
>
  extends Source<VALUE[]>
  implements Transformer<INPUT, VALUE[]>
{
  constructor(
    readonly $input: INPUT,
    private callback: (value: VALUE) => void,
  ) {
    super();
  }
  consume(options?: Consumer.Options<VALUE[]>): Consumer<VALUE[]> {
    return this.$input.consume(new ConsumerOptions(this.callback, options));
  }
}

export function tapBatch<
  INPUT extends Consumable<Array<any>>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
>(callback: (value: VALUE) => void) {
  return ($input: INPUT) => new TapBatch($input, callback);
}

class ConsumerOptions<T> extends Consumer.DefaultOptions<T[]> {
  constructor(
    private callback: (values: T) => void,
    options?: Consumer.Options<T[]>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<T[]>, values: T[]): void {
    for (let i = 0, len = values.length; i < len; i++) {
      this.callback(values[i]);
    }
    super.handler(consumer, values);
  }
}

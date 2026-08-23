import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Transformer } from "../core/types";

export class TapBatch<
  INPUT extends Consumable<Array<any>>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
>
  extends Source<VALUE[]>
  implements Transformer<INPUT, VALUE[]>
{
  constructor(
    readonly $input: INPUT,
    private callback: (value: VALUE, INPUT: INPUT) => void,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE[]>, options?: Consumer.Options<VALUE[]>): Consumer<VALUE[]> {
    return this.$input.consume((consumer, values) => {
      for (let i = 0, len = values.length; i < len; i++) {
        this.callback(values[i], this.$input);
      }
      handler(consumer, values);
    }, options);
  }
}

export function tapBatch<
  INPUT extends Consumable<Array<any>>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
>(callback: (value: VALUE, INPUT: INPUT) => void) {
  return ($input: INPUT) => new TapBatch($input, callback);
}

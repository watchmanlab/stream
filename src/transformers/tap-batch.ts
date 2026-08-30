import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfArray, ValueOfConsumable } from "../core/types";

export class TapBatch<
  INPUT extends Consumable<Array<any>>,
  VALUE extends ValueOfArray<ValueOfConsumable<INPUT>> = ValueOfArray<ValueOfConsumable<INPUT>>,
> extends Source<VALUE[]> {
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
  VALUE extends ValueOfArray<ValueOfConsumable<INPUT>> = ValueOfArray<ValueOfConsumable<INPUT>>,
>(callback: (value: VALUE, INPUT: INPUT) => void) {
  return ($input: INPUT) => new TapBatch($input, callback);
}

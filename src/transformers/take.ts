import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Take<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private count: number,
  ) {
    super();
  }

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      if (this.count--) {
        handler(consumer, value);
      } else {
        consumer.terminate("complete");
      }
    }, options);
  }
}

export function take<INPUT extends Consumable.AnyConsumable>(count: number) {
  return ($input: INPUT) => new Take($input, count);
}

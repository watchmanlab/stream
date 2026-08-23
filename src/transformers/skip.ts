import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Skip<
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
    let count = this.count;
    return this.$input.consume((consumer, value) => {
      if (count--) {
        consumer.next();
      } else {
        consumer["_handler"] = handler;
        handler(consumer, value);
      }
    }, options);
  }
}

export function skip<INPUT extends Consumable.AnyConsumable>(count: number) {
  return ($input: INPUT) => new Skip($input, count);
}

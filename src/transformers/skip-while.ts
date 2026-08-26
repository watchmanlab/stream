import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class SkipWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private predicate: (value: VALUE) => boolean,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      if (this.predicate(value)) {
        consumer.next();
      } else {
        consumer.handler = handler;
        handler(consumer, value);
      }
    }, options);
  }
}

export function skipWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new SkipWhile($input, predicate);
}

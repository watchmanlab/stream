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

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      if (this.predicate(value)) {
        handler(consumer, value);
      } else {
        consumer.terminate("complete");
      }
    }, options);
  }
}

export function takeWhile<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new TakeWhile($input, predicate);
}

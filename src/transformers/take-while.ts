import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class TakeWhile<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
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

export function takeWhile<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  predicate: (value: VALUE) => boolean,
) {
  return ($input: INPUT) => new TakeWhile($input, predicate);
}

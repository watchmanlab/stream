import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class SkipWhile<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private predicate: (value: VALUE) => boolean,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$input.consume(
      (consumer, value) => {
        if (this.predicate(value)) {
          consumer.next();
        } else {
          consumer["_handler"] = handler;
          handler(consumer, value);
        }
      },
      { ...options },
    );
  }
}

export function skipWhile<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  predicate: (value: VALUE) => boolean,
) {
  return ($input: INPUT) => new SkipWhile($input, predicate);
}

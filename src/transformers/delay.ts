import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class Delay<
  INPUT extends AnyConsumable,
  MS extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private ms: MS,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      setTimeout(() => {
        handler(consumer, value);
      }, this.ms);
    }, options);
  }
}

export function delay<INPUT extends AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Delay($input, ms);
}

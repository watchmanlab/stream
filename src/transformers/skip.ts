import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class Skip<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private count: number,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      if (this.count--) {
        consumer.next();
      } else {
        consumer["_handler"] = handler;
        handler(consumer, value);
      }
    }, options);
  }
}

export function skip<INPUT extends AnyConsumable>(count: number) {
  return ($input: INPUT) => new Skip($input, count);
}

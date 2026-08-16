import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class Take<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
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
        handler(consumer, value);
      } else {
        consumer.terminate("complete");
      }
    }, options);
  }
}

export function take<INPUT extends AnyConsumable>(count: number) {
  return ($input: INPUT) => new Take($input, count);
}

import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, AnyStream, ExtractValue, Transformer } from "../core/types";

export class Tick<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(readonly $input: INPUT) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => queueMicrotask(() => handler(consumer, value)), options);
  }
}

export function tick<INPUT extends AnyConsumable>() {
  return ($input: INPUT) => new Tick($input);
}

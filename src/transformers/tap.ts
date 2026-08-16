import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";

import { AnyConsumable, AnyStream, ExtractValue, NonEmptyString, Transformer } from "../core/types";

export class Tap<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private callback: (value: VALUE, input: INPUT) => void,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$input.consume((consumer, value) => {
      this.callback(value, this.$input);
      handler(consumer, value);
    }, options);
  }
}
export function tap<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  callback: (value: VALUE, INPUT: INPUT) => void,
) {
  return ($input: INPUT) => new Tap($input, callback);
}

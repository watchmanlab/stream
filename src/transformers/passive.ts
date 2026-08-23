import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Transformer } from "../core/types";

export class Passive<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(readonly $input: INPUT) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    options ? (options.passive = true) : (options = { passive: true });
    return this.$input.consume(handler, options);
  }
}

export function passive<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Passive($input);
}

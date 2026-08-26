import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class DelayOnce<
  INPUT extends Consumable.AnyConsumable,
  MS extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private ms: MS,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    let ms = this.ms;
    return this.$input.consume((c, v) => {
      setTimeout(() => {
        handler(c, v);
        c.handler = handler;
      }, ms);
    }, options);
  }
}

export function delayOnce<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new DelayOnce($input, ms);
}

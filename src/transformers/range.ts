import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class Range<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private start: number,
    private offset: number,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    let index = 0;

    return this.$input.consume((c, v) => {
      if (++index < this.start) {
        c.next();
      } else if (index < this.start + this.offset) {
        handler(c, v);
      } else {
        c.terminate("complete");
      }
    }, options);
  }
}

export function range<INPUT extends Consumable.AnyConsumable>(start: number, offset: number) {
  return ($input: INPUT) => new Range($input, start, offset);
}

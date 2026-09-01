import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class Min<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(handler: Consumer.Handler<number>, options?: Consumer.Options<number>): Consumer<number> {
    let min = null as number | null;

    return this.$input.consume((c, v) => {
      if (!min) {
        min = v;
      } else {
        min = min < v ? min : v;
      }
      handler(c, min);
    }, options);
  }
}

export function min<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Min($input);
}

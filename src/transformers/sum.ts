import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class Sum<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(handler: Consumer.Handler<number>, options?: Consumer.Options<number>): Consumer<number> {
    let total = 0;

    return this.$input.consume((c, v) => {
      total += v;
      handler(c, total);
    }, options);
  }
}

export function sum<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Sum($input);
}

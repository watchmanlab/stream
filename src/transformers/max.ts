import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class Max<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    let max = null as number | null;

    return this.$input.consume((c, v) => {
      if (!max) {
        max = v;
      } else {
        max = max < v ? v : max;
      }
      handler(c, max);
    }, options);
  }
}

export function max<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Max($input);
}

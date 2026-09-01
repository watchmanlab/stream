import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class Count<INPUT extends Consumable.AnyConsumable> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    let count = 0;

    return this.$input.consume((c, v) => {
      handler(c, ++count);
    }, options);
  }
}

export function count<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Count($input);
}

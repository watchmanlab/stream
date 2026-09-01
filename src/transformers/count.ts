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
    const { terminate, ...rest } = options ?? {};

    let count = 0;

    return this.$input.consume(
      (c, v) => {
        count++;
        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          handler(c, count);
          terminate?.(c, r);
        },
      },
    );
  }
}

export function count<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Count($input);
}

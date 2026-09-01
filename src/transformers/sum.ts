import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Sum<INPUT extends Consumable<number>> extends Source<number> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number>,
    options?: Consumer.Options<number> | undefined,
  ): Consumer<number> {
    const { terminate, ...rest } = options ?? {};

    let total = 0;

    return this.$input.consume(
      (c, v) => {
        total += v;
        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          handler(c, total);
          terminate?.(c, r);
        },
      },
    );
  }
}

export function sum<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Sum($input);
}

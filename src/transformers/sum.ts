import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Sum<INPUT extends Consumable<number>> extends Source<number | Result<number>> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<number | Result<number>>,
    options?: Consumer.Options<number | Result<number>> | undefined,
  ): Consumer<number | Result<number>> {
    const { terminate, ...rest } = (options ?? {}) as Consumer.Options<any>;

    let total = 0;

    return this.$input.consume(
      (c: Consumer<any>, v) => {
        total += v;
        handler(c, total);
      },
      {
        ...rest,
        terminate(c: Consumer<any>, r) {
          handler(c, new Result(total));
          terminate?.(c, r);
        },
      },
    ) as never;
  }
}

export function sum<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Sum($input);
}

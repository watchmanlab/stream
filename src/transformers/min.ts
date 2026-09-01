import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Min<INPUT extends Consumable<number>> extends Source<Result<number, "not-found">> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<number, "not-found">>,
    options?: Consumer.Options<Result<number, "not-found">> | undefined,
  ): Consumer<Result<number, "not-found">> {
    const { terminate, ...rest } = (options ?? {}) as Consumer.Options<any>;

    let min = null as number | null;

    return this.$input.consume(
      (c, v) => {
        if (!min) {
          min = v;
        } else {
          min = min < v ? min : v;
        }
        c.next();
      },
      {
        ...rest,
        terminate(c: Consumer<any>, r) {
          min ? handler(c, { ok: true, value: min }) : handler(c, { ok: false, error: "not-found" });
          terminate?.(c, r);
        },
      },
    ) as never;
  }
}

export function min<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Min($input);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class Max<INPUT extends Consumable<number>> extends Source<Result<number, "not-found">> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<number, "not-found">>,
    options?: Consumer.Options<Result<number, "not-found">> | undefined,
  ): Consumer<Result<number, "not-found">> {
    const { terminate, ...rest } = (options ?? {}) as Consumer.Options<any>;

    let max = null as number | null;

    return this.$input.consume(
      (c, v) => {
        if (!max) {
          max = v;
        } else {
          max = max < v ? v : max;
        }
        c.next();
      },
      {
        ...rest,
        terminate(c: Consumer<any>, r) {
          max ? handler(c, { ok: true, value: max }) : handler(c, { ok: false, error: "not-found" });
          terminate?.(c, r);
        },
      },
    ) as never;
  }
}

export function max<INPUT extends Consumable<number>>() {
  return ($input: INPUT) => new Max($input);
}

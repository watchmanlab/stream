import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ExtractValue, Result } from "../core/types";

export class Last<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<Result<VALUE, "not-found">> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<VALUE, "not-found">>,
    options?: Consumer.Options<Result<VALUE, "not-found">> | undefined,
  ): Consumer<Result<VALUE, "not-found">> {
    const { terminate, ...rest } = options ?? {};
    let value: VALUE | Empty = EMPTY;

    return this.$input.consume(
      (c, v) => {
        value = v;
        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          value === EMPTY ? handler(c, { ok: false, error: "not-found" }) : handler(c, { ok: true, value });
          terminate?.(c, r);
        },
      },
    );
  }
}

export function last<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Last($input);
}

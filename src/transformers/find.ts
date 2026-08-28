import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Result } from "../core/types";

export class Find<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<Result<VALUE, "not-found">> {
  constructor(
    private $input: INPUT,
    private predicate: (value: VALUE, index: number) => boolean,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<Result<VALUE, "not-found">>,
    options?: Consumer.Options<Result<VALUE, "not-found">> | undefined,
  ): Consumer<Result<VALUE, "not-found">> {
    const { terminate, ...rest } = options ?? {};
    const { predicate } = this;
    let found = false;
    let index = 0;
    return this.$input.consume(
      (c, value) => {
        if (predicate(value, index++)) {
          found = true;
          c.terminate("complete");
          handler(c, { ok: true, value });
        } else {
          c.next();
        }
      },
      {
        ...rest,
        terminate(c, r) {
          if (!found) handler(c, { ok: false, error: "not-found" });
          terminate?.(c, r);
        },
      },
    );
  }
}

export function find<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  predicate: (value: VALUE, index: number) => boolean,
) {
  return ($input: INPUT) => new Find($input, predicate);
}

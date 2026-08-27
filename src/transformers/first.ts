import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Result } from "../core/types";

export class First<
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
    let found = false;

    return this.$input.consume(
      (c, value) => {
        found = true;
        c.terminate("complete");
        handler(c, { ok: true, value });
      },
      {
        ...rest,
        terminate(consumer, reason) {
          if (!found) handler(consumer, { ok: false, error: "not-found" });
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function first<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new First($input);
}

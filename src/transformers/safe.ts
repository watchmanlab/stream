import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, isResult, Result } from "../core/types";

export class Safe<
  INPUT extends Consumable.AnyConsumable,
  OUTPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<OUTPUT> = ExtractValue<OUTPUT>,
> extends Source<Result<VALUE, any>> {
  constructor(
    private $input: INPUT,
    private fn: ($input: INPUT) => OUTPUT,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<Result<VALUE, any>>,
    options?: Consumer.Options<Result<VALUE, any>> | undefined,
  ): Consumer<Result<VALUE, any>> {
    const $output = this.fn(this.$input);

    return $output.consume((c, v) => {
      try {
        handler(c, { ok: true, value: v });
      } catch (error) {
        handler(c, { ok: false, error });
      }
    }, options);
  }
}

export function safe<INPUT extends Consumable.AnyConsumable, OUTPUT extends Consumable.AnyConsumable>(
  fn: ($input: INPUT) => OUTPUT,
) {
  return ($input: INPUT) => new Safe($input, fn);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Result as ResultType } from "../core/types";

export class Result<
  INPUT extends Consumable.AnyConsumable,
  OUTPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<OUTPUT> = ExtractValue<OUTPUT>,
> extends Source<ResultType<VALUE, any>> {
  constructor(
    private $input: INPUT,
    private fn: ($input: INPUT) => OUTPUT,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<ResultType<VALUE, any>>,
    options?: Consumer.Options<ResultType<VALUE, any>> | undefined,
  ): Consumer<ResultType<VALUE, any>> {
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

export function result<INPUT extends Consumable.AnyConsumable, OUTPUT extends Consumable.AnyConsumable>(
  fn: ($input: INPUT) => OUTPUT,
) {
  return ($input: INPUT) => new Result($input, fn);
}

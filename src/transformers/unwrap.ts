import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Result } from "../core/types";

export class Unwrap<
  INPUT extends Consumable<Result<any, any>>,
  RESULT extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<RESULT["value"]> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<RESULT["value"]>,
    options?: Consumer.Options<RESULT["value"]> | undefined,
  ): Consumer<RESULT["value"]> {
    return this.$input.consume((c, v) => {
      if (v.ok) handler(c, v.value);
      else c.next();
    }, options);
  }
}

export function unwrap<INPUT extends Consumable<Result<any, any>>>() {
  return ($input: INPUT) => new Unwrap($input);
}

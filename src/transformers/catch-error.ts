import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue, Result } from "../core/types";

export class CatchError<
  INPUT extends Consumable<Result<any, any>>,
  RESULT extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<RESULT["value"]> {
  constructor(
    private $input: INPUT,
    private callback?: (error: RESULT["error"]) => void,
  ) {
    super();
  }

  override consume(
    handler: Consumer.Handler<RESULT["value"]>,
    options?: Consumer.Options<RESULT["value"]> | undefined,
  ): Consumer<RESULT["value"]> {
    const { callback } = this;

    return this.$input.consume((c, v) => {
      if (v.ok) {
        handler(c, v.value);
      } else {
        callback?.(v.error);
        c.next();
      }
    }, options);
  }
}

export function catchError<
  INPUT extends Consumable<Result<any, any>>,
  RESULT extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>(callback?: (error: RESULT["error"]) => void) {
  return ($input: INPUT) => new CatchError($input, callback);
}

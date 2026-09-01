import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable, Result as ResultType, Error } from "../core/types";

export class Safe<
  INPUT extends Consumable.AnyConsumable,
  OUTPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<OUTPUT> = ValueOfConsumable<OUTPUT>,
> extends Source<VALUE | Error<any>> {
  constructor(
    private $input: INPUT,
    private fn: ($input: INPUT) => OUTPUT,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE | Error<any>>,
    options?: Consumer.Options<VALUE | Error<any>> | undefined,
  ): Consumer<VALUE | Error<any>> {
    const $output = this.fn(this.$input);

    return $output.consume((c, v) => {
      try {
        handler(c, v);
      } catch (error) {
        handler(c, new Error(error));
      }
    }, options);
  }
}

export function safe<INPUT extends Consumable.AnyConsumable, OUTPUT extends Consumable.AnyConsumable>(
  fn: ($input: INPUT) => OUTPUT,
) {
  return ($input: INPUT) => new Safe($input, fn);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { ExtractValue } from "../core/types";

export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>(mapper: (value: VALUE) => MAPPED) {
  return ($input: INPUT) =>
    (handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>): Consumer<MAPPED> =>
      $input.consume((consumer, value) => handler(consumer, mapper(value)), options);
}

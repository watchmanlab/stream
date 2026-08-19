import { Consumable } from "./consumable0";
import { Consumer } from "./consumer0";
import { ExtractValue } from "./types0";

export function map<
  $INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<$INPUT> = ExtractValue<$INPUT>,
  MAPPED = VALUE,
>(mapper: (value: VALUE) => MAPPED): ($input: $INPUT) => Consumable<MAPPED> {
  return ($input: $INPUT) => ({
    consume: (handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>) =>
      $input.consume((c, v) => handler(c, mapper(v)), options),
  });
}

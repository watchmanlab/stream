import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  MAPPED = VALUE,
> extends Source<MAPPED> {
  constructor(
    readonly $input: INPUT,
    private mapper: (value: VALUE, index: number) => MAPPED,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>): Consumer<MAPPED> {
    let index = 0;

    return this.$input.consume((c, v) => handler(c, this.mapper(v, index++)), options);
  }
}

export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  MAPPED = VALUE,
>(mapper: (value: VALUE, index: number) => MAPPED) {
  return ($input: INPUT) => new Map($input, mapper);
}

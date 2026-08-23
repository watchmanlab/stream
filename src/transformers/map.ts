import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { ExtractValue } from "../core/types";

export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>(mapper: (value: VALUE) => MAPPED) {
  return ($input: INPUT) =>
    (consumer: Consumer<VALUE>): Consumer<MAPPED> =>
      $input.consume(new InputConsumer(consumer, mapper));
}

class InputConsumer extends Consumer<any> {
  constructor(
    private consumer: Consumer<any>,
    private mapper: (value: any) => any,
  ) {
    super();
  }
  protected override handler(consumer: Consumer<any>, value: any): void {
    Consumer.push(this.consumer, this.mapper(value));
  }
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Scan<
  INPUT extends Consumable.AnyConsumable,
  ACC,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<ACC> {
  constructor(
    private $input: INPUT,
    private acc: ACC,
    private reducer: (acc: ACC, value: VALUE) => ACC,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<ACC>, options?: Consumer.Options<ACC> | undefined): Consumer<ACC> {
    let { acc, reducer } = this;
    return this.$input.consume((c, v) => {
      acc = reducer(acc, v);
      handler(c, acc);
    }, options);
  }
}

export function scan<
  INPUT extends Consumable.AnyConsumable,
  ACC,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(acc: ACC, reducer: (acc: ACC, value: VALUE) => ACC) {
  return ($input: INPUT) => new Scan($input, acc, reducer);
}

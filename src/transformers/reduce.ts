import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";
export class Reduce<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  ACC = VALUE,
> extends Source<ACC> {
  constructor(
    private $input: INPUT,
    private acc: ACC,
    private reducer: (acc: ACC, value: VALUE, index: number) => ACC,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<ACC>, options?: Consumer.Options<ACC> | undefined): Consumer<ACC> {
    const { terminate, ...rest } = options ?? {};
    let { reducer, acc } = this;

    let index = 0;

    return this.$input.consume(
      (c, v) => {
        acc = reducer(acc, v, index++);
        c.next();
      },
      {
        ...rest,
        terminate(consumer, reason) {
          handler(consumer, acc);
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function reduce<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  ACC = VALUE,
>(acc: ACC, reducer: (acc: ACC, value: VALUE, index: number) => ACC) {
  return ($input: INPUT) => new Reduce($input, acc, reducer);
}

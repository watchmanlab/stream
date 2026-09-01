import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ValueOfConsumable } from "../core/types";

export class Last<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE | Empty> {
  constructor(private $input: INPUT) {
    super();
  }

  override consume(
    handler: Consumer.Handler<VALUE | Empty>,
    options?: Consumer.Options<VALUE | Empty> | undefined,
  ): Consumer<VALUE | Empty> {
    const { terminate, ...rest } = options ?? {};
    let last: VALUE | Empty = EMPTY;

    return this.$input.consume(
      (c, v) => {
        last = v;
        c.next();
      },
      {
        ...rest,
        terminate(c, r) {
          handler(c, last);
          terminate?.(c, r);
        },
      },
    );
  }
}

export function last<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Last($input);
}

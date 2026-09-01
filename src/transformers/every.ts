import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Every<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<boolean> {
  constructor(
    private $input: INPUT,
    private predicate: (value: VALUE) => boolean,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<boolean>,
    options?: Consumer.Options<boolean> | undefined,
  ): Consumer<boolean> {
    const { terminate, ...rest } = options ?? {};
    const { predicate } = this;
    let ok = true;

    return this.$input.consume(
      (c, v) => {
        if (!predicate(v)) {
          ok = false;
          c.terminate("complete");
        } else {
          c.next();
        }
      },
      {
        ...rest,
        terminate(c, r) {
          handler(c, ok);
          terminate?.(c, r);
        },
      },
    );
  }
}

export function every<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(predicate: (value: VALUE) => boolean) {
  return ($input: INPUT) => new Every($input, predicate);
}

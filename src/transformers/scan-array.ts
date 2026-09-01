import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

class ScanArray<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE[]> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE[]>,
    options?: Consumer.Options<VALUE[]> | undefined,
  ): Consumer<VALUE[]> {
    const { terminate, ...rest } = options ?? {};
    let array = new Array<VALUE>();

    return this.$input.consume(
      (c, v) => {
        array.push(v);
        handler(c, array);
      },
      {
        ...rest,
        terminate(c, r) {
          (array as any) = null;
          terminate?.(c, r);
        },
      },
    );
  }
}

export function scanArray<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new ScanArray($input);
}

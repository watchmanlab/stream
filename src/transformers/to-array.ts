import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";
import { fromIterable } from "../sources/iterable-source";

class ToArray<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE[]> {
  constructor(private $input: INPUT) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE[]>,
    options?: Consumer.Options<VALUE[]> | undefined,
  ): Consumer<VALUE[]> {
    const { terminate, ...rest } = options ?? {};
    const array = new Array<VALUE>();

    return this.$input.consume(
      (c, v) => {
        array.push(v);
        c.next();
      },
      {
        ...rest,
        terminate(consumer, reason) {
          handler(consumer, array);
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function toArray<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new ToArray($input);
}

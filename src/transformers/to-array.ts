import { Consumable } from "../core/consumable";
import { ExtractValue } from "../core/types";
import { fromIterable } from "../sources/iterable-source";

class Array<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends globalThis.Array<VALUE> {
  asSource() {
    return fromIterable(this);
  }
  print() {
    console.log(this);
  }
}

export function toArray<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>() {
  return async ($input: INPUT) => {
    const array = new Array<VALUE>();
    return new Promise<Array<VALUE>>((resolve) => {
      $input
        .consume(
          (c, v) => {
            array.push(v);
            c.next();
          },
          {
            terminate(consumer, reason) {
              resolve(array);
            },
          },
        )
        .next();
    });
  };
}

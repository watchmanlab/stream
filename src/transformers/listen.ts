import { Consumable } from "../core/consumable";

import { ValueOfConsumable } from "../core/types";

export function listen<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
>(callback?: (value: VALUE) => void) {
  return ($input: INPUT) => {
    $input
      .consume((c, v) => {
        callback?.(v);
        c.next();
      })
      .next();

    return $input;
  };
}

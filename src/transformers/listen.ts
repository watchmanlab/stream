import { Consumable } from "../core/consumable";

import { ExtractValue } from "../core/types";

export function listen<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>(
  callback?: (value: VALUE) => void,
) {
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

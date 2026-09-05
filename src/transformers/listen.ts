import { Consumable } from "../core/consumable";
import { Source } from "../core/source";

import { ValueOfConsumable } from "../core/types";

/**
 * Terminal operator — subscribes and calls `next()` automatically after each value.
 * Returns the input source for further chaining.
 *
 * @param callback just a callback to execute.
 *
 * @example
 * of(1, 2, 3).pipe(listen(console.log));
 */
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

    return $input instanceof Source ? $input : Source.from($input);
  };
}

import { Consumable } from "../core/consumable";

export function pump<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => {
    $input.consume((c) => c.next()).next();
    return $input;
  };
}

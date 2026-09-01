import { Consumable } from "../core/consumable";

export function pipe<INPUT extends Consumable.AnyConsumable>(pipeline: ($input: INPUT) => void) {
  return ($input: INPUT) => {
    pipeline($input);
    return $input;
  };
}

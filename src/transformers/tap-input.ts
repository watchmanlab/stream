import { Consumable } from "../core/consumable";

export function tapInput<INPUT extends Consumable.AnyConsumable>(fn: ($input: INPUT) => void) {
  return ($input: INPUT) => {
    fn($input);
    return $input;
  };
}

import { Consumable } from "../core/consumable";

export class Debounce<INPUT extends Consumable.AnyConsumable, MS extends number> {}

export function debounce<INPUT extends Consumable.AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Debounce();
}

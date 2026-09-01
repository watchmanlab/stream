import { Consumable } from "../core/consumable";
import { Stream } from "../core/stream";
import { ValueOfConsumable } from "../core/types";

export function share<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => Stream.from<ValueOfConsumable<INPUT>>($input);
}

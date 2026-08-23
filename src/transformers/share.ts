import { Consumable } from "../core/consumable";
import { Stream } from "../core/stream";

export function share<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => Stream.from($input);
}

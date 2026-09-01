import { Consumable } from "../core/consumable";
import { Source } from "../core/source";
import { listen } from "./listen";
import { print } from "./print";
import { pump } from "./pump";

export function toConsole<INPUT extends Consumable.AnyConsumable>(label?: string) {
  return ($input: INPUT) => ($input instanceof Source ? $input : Source.from($input)).pipe(print(label)).pipe(pump());
}

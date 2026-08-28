import { Consumable } from "../core/consumable";
import { Source } from "../core/source";
import { listen } from "./listen";

export function toConsole<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => Source.from($input).pipe(listen(console.log));
}

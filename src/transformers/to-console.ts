import { Consumable } from "../core/consumable";
import { Source } from "../core/source";
import { print } from "./print";
import { pump } from "./pump";

/**
 * Shorthand terminal operator — prints each value to the console and drains the stream.
 *
 * @example
 * of(1, 2, 3).pipe(toConsole('result:'));
 */
export function toConsole<INPUT extends Consumable.AnyConsumable>(label?: string) {
  return ($input: INPUT) => ($input instanceof Source ? $input : Source.from($input)).pipe(print(label)).pipe(pump());
}

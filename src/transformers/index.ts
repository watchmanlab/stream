import { Consumable } from "../core/consumable";
import { Map } from "./map";

export function index<INPUT extends Consumable.AnyConsumable>() {
  return ($input: INPUT) => new Map($input, (_, index) => index);
}

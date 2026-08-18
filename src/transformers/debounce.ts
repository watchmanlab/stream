import { AnyConsumable } from "../core/types";
import { KeepNewest } from "./keep-newest";
import { pace } from "./pace";

export function debounce<INPUT extends AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new KeepNewest($input, 1).pipe(pace(ms));
}

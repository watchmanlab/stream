import { Stream } from "../core/stream";
import { AnyConsumable } from "../core/types";

export function share<INPUT extends AnyConsumable>() {
  return ($input: INPUT) => new Stream({ $source: $input });
}

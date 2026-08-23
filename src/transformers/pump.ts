import { AnyConsumable } from "../core/types";

export function pump<INPUT extends AnyConsumable>() {
  return ($input: INPUT) => {
    $input.consume((c) => c.next()).next();
    return $input;
  };
}

import { Consumable } from "../core/types";
import { fromIterator } from "../sources/from-iterator";

export function fromGenerator<VALUE>(generatorFn: () => Generator<VALUE>): Consumable<VALUE> {
  return fromIterator(generatorFn);
}

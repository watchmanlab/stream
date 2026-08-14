import { Consumable } from "../core/types";
import { fromAsyncIterator } from "../sources/from-async-iterator";

export function fromAsyncGenerator<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): Consumable<VALUE> {
  return fromAsyncIterator(asyncGeneratorFn);
}

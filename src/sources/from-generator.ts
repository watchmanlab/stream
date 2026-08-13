import { Source } from "../core/types";
import { fromIterator } from "../sources/from-iterator";

export function fromGenerator<VALUE>(generatorFn: () => Generator<VALUE>): Source<VALUE> {
  return fromIterator(generatorFn);
}

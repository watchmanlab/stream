import { Source } from "../core/types";
import { fromAsyncIterator } from "../sources/from-async-iterator";

export function fromAsyncGenerator<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): Source<VALUE> {
  return fromAsyncIterator(asyncGeneratorFn);
}

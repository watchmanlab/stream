import { FromAsyncIterator } from "../sources/from-async-iterator";
export class FromAsyncGenerator<VALUE> extends FromAsyncIterator<VALUE> {
  constructor(asyncGeneratorFn: () => AsyncGenerator<VALUE>) {
    super(asyncGeneratorFn);
  }
}
export function fromAsyncGenerator<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): FromAsyncGenerator<VALUE> {
  return new FromAsyncGenerator(asyncGeneratorFn);
}

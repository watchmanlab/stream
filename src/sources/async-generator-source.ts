import { AsyncIteratorSource } from "./async-iterator-source";
export class AsyncGeneratorSource<VALUE> extends AsyncIteratorSource<VALUE> {
  constructor(asyncGeneratorFn: () => AsyncGenerator<VALUE>) {
    super(asyncGeneratorFn);
  }
}
export function fromAsyncGenerator<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): AsyncGeneratorSource<VALUE> {
  return new AsyncGeneratorSource(asyncGeneratorFn);
}

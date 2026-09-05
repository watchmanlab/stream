import { AsyncIteratorSource } from "./async-iterator-source";
/**
 * `Replayable`
 *
 * Wraps an async generator function as a pull-based `Source`.
 * A new generator is created per consumer.
 *
 * @example
 * fromAsyncGenerator(async function* () { yield 1; yield 2; }).pipe(listen(console.log));
 */
export class AsyncGeneratorSource<VALUE> extends AsyncIteratorSource<VALUE> {
  constructor(asyncGeneratorFn: () => AsyncGenerator<VALUE>) {
    super(asyncGeneratorFn);
  }
}
/**
 * `Replayable`
 *
 * Wraps an async generator function as a pull-based `Source`.
 * A new generator is created per consumer.
 *
 * @param asyncGeneratorFn () => AsyncGenerator<VALUE>.
 *
 * @example
 * fromAsyncGenerator(async function* () { yield 1; yield 2; })
    .pipe(listen(console.log));
 */
export function fromAsyncGenerator<VALUE>(asyncGeneratorFn: () => AsyncGenerator<VALUE>): AsyncGeneratorSource<VALUE> {
  return new AsyncGeneratorSource(asyncGeneratorFn);
}

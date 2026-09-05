import { IteratorSource } from "./iterator-source";

/**
 * Wraps a generator function as a pull-based `Source`.
 * A new generator is created per consumer.
 *
 * @example
 * fromGenerator(function* () { yield 1; yield 2; }).pipe(listen(console.log));
 */
export class GeneratorSource<VALUE> extends IteratorSource<VALUE> {
  constructor(generatorFn: () => Generator<VALUE>) {
    super(generatorFn);
  }
}
/**
 * Creates a `GeneratorSource` from a generator function.
 * @param generatorFn Factory that returns a new `Generator` per consumer.
 */
export function fromGenerator<VALUE>(generatorFn: () => Generator<VALUE>): GeneratorSource<VALUE> {
  return new GeneratorSource(generatorFn);
}

import { IteratorSource } from "./iterator-source";

export class GeneratorSource<VALUE> extends IteratorSource<VALUE> {
  constructor(generatorFn: () => Generator<VALUE>) {
    super(generatorFn);
  }
}
export function fromGenerator<VALUE>(generatorFn: () => Generator<VALUE>): GeneratorSource<VALUE> {
  return new GeneratorSource(generatorFn);
}

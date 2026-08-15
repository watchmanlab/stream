import { FromIterator } from "../sources/from-iterator";

export class FromGenerator<VALUE> extends FromIterator<VALUE> {
  constructor(generatorFn: () => Generator<VALUE>) {
    super(generatorFn);
  }
}
export function fromGenerator<VALUE>(generatorFn: () => Generator<VALUE>): FromGenerator<VALUE> {
  return new FromGenerator(generatorFn);
}

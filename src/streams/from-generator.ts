import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { FromIterator } from "./from-iterator";

export class FromGenerator<VALUE, NAME extends NonEmptyString = "$generator"> extends FromIterator<VALUE, NAME> {
  constructor(generatorFn: () => Generator<VALUE>, options?: Stream.Options<VALUE, NAME>) {
    super(generatorFn, { ...options, name: options?.name ?? ("$generator" as NAME) });
  }
}

export function fromGenerator<VALUE, NAME extends NonEmptyString = "$generator">(
  generatorFn: () => Generator<VALUE>,
  options?: Stream.Options<VALUE, NAME>,
): FromGenerator<VALUE, NAME> {
  return new FromGenerator(generatorFn, options);
}

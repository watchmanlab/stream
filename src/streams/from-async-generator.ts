import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { FromAsyncIterator } from "./from-async-iterator";

export class FromAsyncGenerator<VALUE, NAME extends NonEmptyString = "$asyncGenerator"> extends FromAsyncIterator<
  VALUE,
  NAME
> {
  constructor(asyncGeneratorFn: () => AsyncGenerator<VALUE>, options?: Stream.Options<VALUE, NAME>) {
    super(asyncGeneratorFn, {
      ...options,
      name: options?.name ?? ("$asyncGenerator" as NAME),
    });
  }
}

export function fromAsyncGenerator<VALUE, NAME extends NonEmptyString = "$asyncGenerator">(
  asyncGeneratorFn: () => AsyncGenerator<VALUE>,
  options?: Stream.Options<VALUE, NAME>,
): FromAsyncGenerator<VALUE, NAME> {
  return new FromAsyncGenerator(asyncGeneratorFn, options);
}

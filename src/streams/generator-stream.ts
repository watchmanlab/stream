import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { IteratorStream } from "./iterator-stream";

export class GeneratorStream<VALUE, NAME extends NonEmptyString = "$generator"> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly functionGenerator: () => Generator<VALUE>,
    options?: Stream.Options<VALUE, NAME>,
  ) {
    super(functionGenerator, { ...options, name: options?.name ?? ("$generator" as NAME) });
  }
}

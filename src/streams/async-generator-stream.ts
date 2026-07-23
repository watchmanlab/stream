import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncGeneratorStream<VALUE, NAME extends NonEmptyString = "$asyncGenerator"> extends AsyncIteratorStream<
  VALUE,
  NAME
> {
  constructor(
    public readonly asyncFunctionGenerator: () => AsyncGenerator<VALUE>,
    options?: Stream.Options<VALUE, NAME>,
  ) {
    super(asyncFunctionGenerator, {
      ...options,
      name: options?.name ?? ("$asyncGenerator" as NAME),
    });
  }
}

import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { IteratorStream } from "./iterator-stream";

export class GeneratorStream<VALUE, NAME extends NonEmptyString> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly functionGenerator: () => Generator<VALUE>,
    options?: GeneratorStream.Options<VALUE, NAME>,
  ) {
    super(functionGenerator, options);
  }
}
export namespace GeneratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncGeneratorStream<VALUE, NAME extends NonEmptyString> extends AsyncIteratorStream<VALUE, NAME> {
  constructor(
    public readonly asyncFunctionGenerator: () => AsyncGenerator<VALUE>,
    options?: AsyncGeneratorStream.Options<VALUE, NAME>,
  ) {
    super(asyncFunctionGenerator, options);
  }
}

export namespace AsyncGeneratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

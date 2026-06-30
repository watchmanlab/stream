import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncIterableStream<VALUE, NAME extends NonEmptyString> extends AsyncIteratorStream<VALUE, NAME> {
  constructor(
    public readonly asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
    options?: AsyncIterableStream.Options<VALUE, NAME>,
  ) {
    super(
      () =>
        typeof asyncIterable === "function"
          ? asyncIterable()[Symbol.asyncIterator]()
          : asyncIterable[Symbol.asyncIterator](),
      options,
    );
  }
}

export namespace AsyncIterableStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

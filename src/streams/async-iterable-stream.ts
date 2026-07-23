import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncIterableStream<VALUE, NAME extends NonEmptyString = "$asyncIterable"> extends AsyncIteratorStream<
  VALUE,
  NAME
> {
  constructor(
    public readonly asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
    options?: Stream.Options<VALUE, NAME>,
  ) {
    super(
      () =>
        typeof asyncIterable === "function"
          ? asyncIterable()[Symbol.asyncIterator]()
          : asyncIterable[Symbol.asyncIterator](),
      {
        ...options,
        name: options?.name ?? ("$asyncIterable" as NAME),
      },
    );
  }
}

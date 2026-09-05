import { AsyncIteratorSource } from "./async-iterator-source";

/**
 * Wraps an `AsyncIterable` as a pull-based `Source`.
 * A new async iterator is created per consumer.
 *
 * @example
 * fromAsyncIterable(asyncIterable).pipe(listen(console.log));
 */
export class AsyncIterableSource<VALUE> extends AsyncIteratorSource<VALUE> {
  constructor(asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>)) {
    super(() =>
      typeof asyncIterable === "function"
        ? asyncIterable()[Symbol.asyncIterator]()
        : asyncIterable[Symbol.asyncIterator](),
    );
  }
}

/**
 * Creates an `AsyncIterableSource` from an async iterable or factory.
 */
export function fromAsyncIterable<VALUE>(
  asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
): AsyncIterableSource<VALUE> {
  return new AsyncIterableSource(asyncIterable);
}

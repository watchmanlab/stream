import { AsyncIteratorSource } from "./async-iterator-source";

export class AsyncIterableSource<VALUE> extends AsyncIteratorSource<VALUE> {
  constructor(asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>)) {
    super(() =>
      typeof asyncIterable === "function"
        ? asyncIterable()[Symbol.asyncIterator]()
        : asyncIterable[Symbol.asyncIterator](),
    );
  }
}

export function fromAsyncIterable<VALUE>(
  asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
): AsyncIterableSource<VALUE> {
  return new AsyncIterableSource(asyncIterable);
}

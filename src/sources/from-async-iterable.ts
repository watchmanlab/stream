import { FromAsyncIterator } from "../sources/from-async-iterator";

export class FromAsyncIterable<VALUE> extends FromAsyncIterator<VALUE> {
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
): FromAsyncIterable<VALUE> {
  return new FromAsyncIterable(asyncIterable);
}

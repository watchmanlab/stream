import { Source } from "../core/types";
import { fromAsyncIterator } from "../sources/from-async-iterator";

export function fromAsyncIterable<VALUE>(
  asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
): Source<VALUE> {
  return fromAsyncIterator(() =>
    typeof asyncIterable === "function"
      ? asyncIterable()[Symbol.asyncIterator]()
      : asyncIterable[Symbol.asyncIterator](),
  );
}

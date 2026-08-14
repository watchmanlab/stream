import { Consumable } from "../core/types";
import { fromAsyncIterator } from "../sources/from-async-iterator";

export function fromAsyncIterable<VALUE>(
  asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
): Consumable<VALUE> {
  return fromAsyncIterator(() =>
    typeof asyncIterable === "function"
      ? asyncIterable()[Symbol.asyncIterator]()
      : asyncIterable[Symbol.asyncIterator](),
  );
}

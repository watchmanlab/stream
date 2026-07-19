import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { fromIterator } from "./from-iterator";

export function fromIterable<VALUE, NAME extends NonEmptyString = "root">(
  iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
) {
  return fromIterator(
    () => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()),
    options,
  );
}

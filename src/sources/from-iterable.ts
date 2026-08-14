import { Consumable } from "../core/types";
import { fromIterator } from "../sources/from-iterator";

export function fromIterable<VALUE>(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)): Consumable<VALUE> {
  return fromIterator(() =>
    typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator](),
  );
}

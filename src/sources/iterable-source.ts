import { IteratorSource } from "./iterator-source";
export class IterableSource<VALUE> extends IteratorSource<VALUE> {
  constructor(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()));
  }
}
export function fromIterable<VALUE>(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)): IterableSource<VALUE> {
  return new IterableSource(iterable);
}

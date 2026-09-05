import { IteratorSource } from "./iterator-source";
/**
 * Wraps any `Iterable` as a pull-based `Source`.
 * A new iterator is created per consumer via the iterable's `Symbol.iterator`.
 *
 * @example
 * fromIterable([1, 2, 3]).pipe(listen(console.log));
 * fromIterable(() => new Set([1, 2, 3])); // factory form
 */
export class IterableSource<VALUE> extends IteratorSource<VALUE> {
  constructor(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()));
  }
}
/**
 * Creates an `IterableSource` from an iterable or iterable factory.
 * @param iterable An `Iterable` or a factory function returning one.
 */
export function fromIterable<VALUE>(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)): IterableSource<VALUE> {
  return new IterableSource(iterable);
}

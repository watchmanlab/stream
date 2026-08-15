import { FromIterator } from "../sources/from-iterator";
export class FromIterable<VALUE> extends FromIterator<VALUE> {
  constructor(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()));
  }
}
export function fromIterable<VALUE>(iterable: Iterable<VALUE> | (() => Iterable<VALUE>)): FromIterable<VALUE> {
  return new FromIterable(iterable);
}

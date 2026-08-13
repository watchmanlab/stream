import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { FromIterator } from "../sources/from-iterator";

export class FromIterable<VALUE, NAME extends NonEmptyString = "$iterable"> extends FromIterator<VALUE, NAME> {
  constructor(iterable: Iterable<VALUE> | (() => Iterable<VALUE>), options?: Stream.Options<VALUE, NAME>) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()), {
      ...options,
      name: options?.name ?? ("$iterable" as NAME),
    });
  }
}

export function fromIterable<VALUE, NAME extends NonEmptyString = "$iterable">(
  iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): FromIterable<VALUE, NAME> {
  return new FromIterable(iterable, options);
}

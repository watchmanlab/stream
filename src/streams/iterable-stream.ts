import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { IteratorStream } from "./iterator-stream";

export class IterableStream<VALUE, NAME extends NonEmptyString = "$iterable"> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
    options?: Stream.Options<VALUE, NAME>,
  ) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()), {
      ...options,
      name: options?.name ?? ("$iterable" as NAME),
    });
  }
}

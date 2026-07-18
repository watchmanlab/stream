import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { IteratorStream } from "./from-iterator";

export class IterableStream<VALUE, NAME extends NonEmptyString> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
    options?: IterableStream.Options<VALUE, NAME>,
  ) {
    super(
      () => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()),
      options,
    );
  }
}

export namespace IterableStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

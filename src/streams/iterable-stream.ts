import { Stream } from "../core/stream";
import { IteratorStream } from "./iterator-stream";

export class IterableStream<VALUE, NAME extends string> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
    init?: Omit<Stream.Init<VALUE, NAME>, "source">,
  ) {
    super(() => (typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]()), init);
  }
}

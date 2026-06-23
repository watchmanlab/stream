import { Stream } from "../core/stream";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncIterableStream<VALUE, NAME extends string> extends AsyncIteratorStream<VALUE, NAME> {
  constructor(
    public readonly asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
    init?: Omit<Stream.Init<VALUE, NAME>, "source">,
  ) {
    super(
      () =>
        typeof asyncIterable === "function"
          ? asyncIterable()[Symbol.asyncIterator]()
          : asyncIterable[Symbol.asyncIterator](),
      init,
    );
  }
}

import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class AsyncIteratorStream<VALUE, NAME extends NonEmptyString = "$asyncIterator"> extends Stream<VALUE, NAME> {
  constructor(
    public readonly asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
    options?: AsyncIteratorStream.Options<VALUE, NAME>,
  ) {
    const iter = typeof asyncItrator === "function" ? asyncItrator() : asyncItrator;

    super({
      ...options,
      name: options?.name ?? ("$asyncIterator" as NAME),
      next(stream, consumer) {
        iter.next().then((result) => {
          if (result.done) {
            stream.terminate("complete");
          } else {
            stream.push(result.value);
          }
        });
        options?.next?.(stream, consumer);
      },
      terminate(stream, reason) {
        iter.return?.();
        options?.terminate?.(stream, reason);
      },
    });
  }
}

export namespace AsyncIteratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

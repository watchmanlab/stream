import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export function fromIterator<VALUE, NAME extends NonEmptyString = "root">(
  iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): Stream<VALUE, NAME> {
  const iter = typeof iterator === "function" ? iterator() : iterator;

  return new Stream({
    ...options,
    pull(stream, consumer) {
      const result = iter.next();
      if (result.done) {
        stream.terminate("complete");
      } else {
        stream.push(result.value);
        options?.pull?.(stream, consumer);
      }
    },
    terminated(stream, reason) {
      iter.return?.();
      options?.terminated?.(stream, reason);
    },
  });
}

export namespace IteratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

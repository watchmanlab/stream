import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export function fromIterator<VALUE, NAME extends NonEmptyString = "root">(
  iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): Stream<VALUE, NAME> {
  const iter = typeof iterator === "function" ? iterator() : iterator;

  return new Stream({
    ...options,
    next(stream, consumer) {
      const result = iter.next();
      if (result.done) {
        stream.terminate("complete");
      } else {
        stream.push(result.value);
        options?.next?.(stream, consumer);
      }
    },
    terminate(stream, reason) {
      iter.return?.();
      options?.terminate?.(stream, reason);
    },
  });
}

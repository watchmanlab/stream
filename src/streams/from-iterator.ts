import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export function fromIterator<VALUE, NAME extends NonEmptyString = "root">(
  iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): Stream<VALUE, NAME> {
  const iter = typeof iterator === "function" ? iterator() : iterator;
  let started = false;
  return new Stream({
    ...options,
    pull(stream) {
      if (!started) {
        started = true;
        queueMicrotask(() => {
          const result = iter.next();
          if (result.done) stream.terminate("complete");
          else stream.push(result.value);
        });
      } else {
        const result = iter.next();
        if (result.done) stream.terminate("complete");
        else stream.push(result.value);
      }
    },
    terminated() {
      iter.return?.();
    },
  });
}

export namespace IteratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}

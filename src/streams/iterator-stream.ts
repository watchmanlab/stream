import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class IteratorStream<VALUE, NAME extends NonEmptyString = "$iterator"> extends Stream<VALUE, NAME> {
  constructor(
    public readonly iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
    options?: Stream.Options<VALUE, NAME>,
  ) {
    const iter = typeof iterator === "function" ? iterator() : iterator;

    super({
      ...options,
      name: options?.name ?? ("$iterator" as NAME),
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
}

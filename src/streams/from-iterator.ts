import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromIterator<VALUE, NAME extends NonEmptyString = "$iterator"> extends Stream<VALUE, NAME> {
  constructor(iterator: Iterator<VALUE> | (() => Iterator<VALUE>), options?: Stream.Options<VALUE, NAME>) {
    const iter = typeof iterator === "function" ? iterator() : iterator;

    super({
      ...options,
      name: options?.name ?? ("$iterator" as NAME),
      next: (stream, consumer) => {
        options?.next?.(stream, consumer);
        const result = iter.next();
        if (result.done) {
          this.terminate("complete");
        } else {
          this.push(result.value);
        }
      },
      terminate(stream, reason) {
        iter.return?.();
        options?.terminate?.(stream, reason);
      },
    });
  }
}

export function fromIterator<VALUE, NAME extends NonEmptyString = "$iterator">(
  iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): FromIterator<VALUE, NAME> {
  return new FromIterator(iterator, options);
}

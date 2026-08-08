import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromIterator<VALUE, NAME extends NonEmptyString = "$iterator"> extends Stream<VALUE, NAME> {
  constructor(iterator: Iterator<VALUE> | (() => Iterator<VALUE>), options?: Stream.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const iter = typeof iterator === "function" ? iterator() : iterator;

    super({
      ...rest,
      name: name ?? ("$iterator" as NAME),
      next: (stream, consumer) => {
        const result = iter.next();

        if (result.done) {
          this.terminate("complete");
        } else {
          this.push(result.value);
        }
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        iter.return?.();
        terminate?.(stream, reason);
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

import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromAsyncIterator<VALUE, NAME extends NonEmptyString = "$asyncIterator"> extends Stream<VALUE, NAME> {
  constructor(
    asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
    options?: Stream.Options<VALUE, NAME>,
  ) {
    const { name, next, terminate, ...rest } = options ?? {};

    const iter = typeof asyncItrator === "function" ? asyncItrator() : asyncItrator;

    super({
      ...rest,
      name: name ?? ("$asyncIterator" as NAME),
      next(stream, consumer) {
        iter.next().then((result) => {
          if (result.done) {
            stream.terminate("complete");
          } else {
            stream.push(result.value);
          }
        });
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        iter.return?.();
        terminate?.(stream, reason);
      },
    });
  }
}

export function fromAsyncIterator<VALUE, NAME extends NonEmptyString = "$asyncIterator">(
  asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): FromAsyncIterator<VALUE, NAME> {
  return new FromAsyncIterator(asyncItrator, options);
}

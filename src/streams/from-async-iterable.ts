import type { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";
import { FromAsyncIterator } from "./from-async-iterator";

export class FromAsyncIterable<VALUE, NAME extends NonEmptyString = "$asyncIterable"> extends FromAsyncIterator<
  VALUE,
  NAME
> {
  constructor(
    asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
    options?: Stream.Options<VALUE, NAME>,
  ) {
    super(
      () =>
        typeof asyncIterable === "function"
          ? asyncIterable()[Symbol.asyncIterator]()
          : asyncIterable[Symbol.asyncIterator](),
      {
        ...options,
        name: options?.name ?? ("$asyncIterable" as NAME),
      },
    );
  }
}

export function fromAsyncIterable<VALUE, NAME extends NonEmptyString = "$asyncIterable">(
  asyncIterable: AsyncIterable<VALUE> | (() => AsyncIterable<VALUE>),
  options?: Stream.Options<VALUE, NAME>,
): FromAsyncIterable<VALUE, NAME> {
  return new FromAsyncIterable(asyncIterable, options);
}

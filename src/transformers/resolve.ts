import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "resolve",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("resolve" as NAME),
      source: {
        listen: (handler, options) => {
          let count = 0;
          return input.listen((self, maybePromise) => {
            if (++count < concurrency) self.next();

            if (maybePromise instanceof Promise) {
              maybePromise
                .then((value) => handler(self, value))
                .catch((error) => self.next(error))
                .finally(() => {
                  count--;
                });
            } else {
              handler(self, maybePromise);
              count--;
            }
          }, options);
        },
      },
    });
  }
}

export function resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "resolve",
>(concurrency = 1, options?: Resolve.Options<VALUE, NAME>): Transform<INPUT, NAME, Resolve<INPUT, VALUE, NAME>> {
  return (input, name) => new Resolve(input, concurrency, { ...options, name: name ?? options?.name });
}

export namespace Resolve {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}

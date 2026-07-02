import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Concurrent<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "concurrent",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, limit: number, options?: Concurrent.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("concurrent" as NAME),
      source: {
        listen: (handler, options) => {
          let count = 0;
          return input.listen((self, maybePromise) => {
            if (++count >= limit) {
              return;
            } else {
              self.next();
            }

            if (maybePromise instanceof Promise) {
              maybePromise
                .then((value) => handler(self, value))
                .catch((error) => self.next(error))
                .finally(() => {
                  count--;
                  self.next();
                });
            } else {
              count--;
              handler(self, maybePromise);
            }
          }, options);
        },
      },
    });
  }
}

export function concurrent<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "concurrent",
>(limit: number, options?: Concurrent.Options<VALUE, NAME>): Transform<INPUT, NAME, Concurrent<INPUT, VALUE, NAME>> {
  return (input, name) => new Concurrent(input, limit, { ...options, name: name ?? options?.name });
}

export namespace Concurrent {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}

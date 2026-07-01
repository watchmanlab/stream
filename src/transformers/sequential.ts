import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyPromise, ExtractValueFromPromise, NonEmptyString } from "../core/types";

export class Sequential<
  INPUT extends Stream.AnyStream,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "sequential",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Sequential.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("sequential" as NAME),
      source: {
        listen: (handler, options) => {
          return input.listen((self, maybePromise) => {
            if (maybePromise instanceof Promise) {
              maybePromise.then((value) => handler(self, value)).catch((error) => self.next(error));
            } else {
              handler(self, maybePromise);
            }
          }, options);
        },
      },
    });
  }
}

export function sequential<
  INPUT extends Stream<AnyPromise, any>,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "sequential",
>(options?: Sequential.Options<VALUE, NAME>): Stream.Transform<INPUT, NAME, Sequential<INPUT, VALUE, NAME>> {
  return (input, name) => new Sequential(input, { ...options, name: name ?? options?.name });
}

export namespace Sequential {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}

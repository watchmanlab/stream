import { Stream } from "../core/stream";

import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Stream.Options<FILTERED, NAME>,
): Transform<INPUT, NAME, Stream<FILTERED, NAME>> {
  return (input) => {
    const output = new Stream({
      ...options,
      name: options?.name ?? ("$filter" as NAME),
      source: {
        consume() {
          return input.consume((self, value) => {
            if (predicate(value)) {
              output.push(value);
            } else {
              self.next();
            }
          });
        },
      },
    });
    return output;
  };
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<VALUE, FILTERED, NAME extends NonEmptyString> = {
    rejected?: (self: Stream<FILTERED, NAME>, value: VALUE) => void;
  };
}

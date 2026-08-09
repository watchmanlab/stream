import { Stream } from "../core/stream";

import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function filter<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
  NAME extends NonEmptyString = "$filter",
>(
  predicate: Filter.Predicate<VALUE, FILTERED>,
  options?: Filter.Options<VALUE, FILTERED, NAME>,
): Transform<INPUT, NAME, Stream<FILTERED, NAME> & { $rejected: Stream<VALUE, `${NAME}Rejected`> }> {
  return (input) => {
    const { name, rejected, ...rest } = options ?? {};

    let $rejected: Stream<VALUE> | undefined;

    const output = new Stream({
      ...rest,
      name: name ?? ("$filter" as NAME),
      source: {
        consume() {
          return input.consume((self, value) => {
            if (predicate(value)) {
              output.push(value);
            } else {
              rejected?.(output, value);
              self.next();
            }
          });
        },
      },
    });
    return Object.defineProperty(output, "$rejected", {
      get() {
        $rejected ??= new Stream({ $terminate: output.$terminate });
        return new Stream({ name: `${output.name}Rejected`, source: $rejected, $terminate: output.$terminate });
      },
    }) as Stream<FILTERED, NAME> & { $rejected: Stream<VALUE, `${NAME}Rejected`> };
  };
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);

  export type Options<VALUE, FILTERED, NAME extends NonEmptyString> = Omit<Stream.Options<FILTERED, NAME>, "source"> & {
    rejected?: (self: Stream<FILTERED, NAME>, value: VALUE) => void;
  };
}

import { Stream } from "../core/stream";

import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Omit<Stream.Options<VALUE, NAME>, "source">,
): Transform<INPUT, NAME, Stream<VALUE, NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const output = new Stream({
      ...rest,
      name: name ?? ("$tap" as NAME),
      source: {
        consume: () =>
          input.consume((_, value) => {
            fn(value, input);
            output.push(value);
          }),
      },
    });

    return output;
  };
}

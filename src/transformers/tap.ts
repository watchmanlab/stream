import { Stream } from "../core/stream";

import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Stream.Options<VALUE, NAME>,
): Transform<INPUT, NAME, Stream<VALUE, NAME>> {
  return (input) => {
    const output = new Stream({
      ...options,
      name: options?.name ?? ("$tap" as NAME),
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

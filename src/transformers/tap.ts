import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, fn: (value: VALUE, INPUT: INPUT) => void, options?: Transformer.Options<VALUE, NAME>) {
    const { name, ...rest } = options ?? {};

    super(input, {
      ...rest,
      name: name ?? ("$tap" as NAME),
      source: {
        consume: () =>
          input.consume((_, value) => {
            fn(value, input);
            this.push(value);
          }),
      },
    });
  }
}

export function tap<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Stream.Options<VALUE, NAME>,
): Transform<INPUT, Tap<INPUT, VALUE, NAME>> {
  return (input) => new Tap(input, fn, options);
}

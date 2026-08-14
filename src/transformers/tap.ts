import { Producer } from "../core/producer";

import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function tap<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$tap",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Omit<Producer.Options<VALUE, NAME>, "source">,
): Transform<INPUT, NAME, Producer<VALUE, NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const output = new Producer({
      ...rest,
      name: name ?? ("$tap" as NAME),
      source: {
        consume: () => input.consume((_, value) => (fn(value, input), output.push(value))),
      },
    });

    return output;
  };
}

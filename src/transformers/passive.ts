import { Producer } from "../core/producer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function passive<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
>(options?: Omit<Producer.Options<VALUE, NAME>, "source">): Transform<INPUT, NAME, Producer<VALUE, NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const output = new Producer({
      ...rest,
      name: name ?? ("$passive" as NAME),
      source: input.$push,
    });

    return output;
  };
}

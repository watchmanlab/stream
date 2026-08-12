import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function passive<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$passive",
>(options?: Omit<Stream.Options<VALUE, NAME>, "source">): Transform<INPUT, NAME, Stream<VALUE, NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const output = new Stream({
      ...rest,
      name: name ?? ("$passive" as NAME),
      source: input.$push,
    });

    return output;
  };
}

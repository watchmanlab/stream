import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function map<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$map",
>(
  mapper: Map.Mapper<VALUE, MAPPED>,
  options?: Omit<Stream.Options<MAPPED, NAME>, "source">,
): Transform<INPUT, NAME, Stream<MAPPED, NAME>> {
  return (input) => {
    const { name, ...rest } = options ?? {};

    const output = new Stream({
      ...rest,
      name: name ?? ("$map" as NAME),
      source: {
        consume() {
          return input.consume((_, value) => output.push(mapper(value)), {
            terminate: (_, reason) => output.terminate(reason),
          });
        },
      },
    });

    return output;
  };
}
export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}

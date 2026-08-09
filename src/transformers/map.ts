import { Stream } from "../core/stream";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export function map<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$map",
>(
  mapper: Map.Mapper<VALUE, MAPPED>,
  options?: Stream.Options<MAPPED, NAME>,
): Transform<INPUT, NAME, Stream<MAPPED, NAME>> {
  return (input) => {
    const output = new Stream({
      ...options,
      name: options?.name ?? ("$map" as NAME),
      source: {
        consume() {
          return input.consume((self, value) => output.push(mapper(value)));
        },
      },
    });

    return output;
  };
}
export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}

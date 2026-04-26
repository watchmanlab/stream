import { Stream } from "./stream9";

const NAME = "map";

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Stream.Transform<INPUT_STREAM, NAME, Stream<MAPPED, NAME>> {
  return (inputStream, name) => {
    return Stream.transformer(
      new Stream<MAPPED, NAME>(name ?? (NAME as NAME), async function* () {
        for await (const value of inputStream) {
          yield mapper(value);
        }
      }),
      inputStream,
    );
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}

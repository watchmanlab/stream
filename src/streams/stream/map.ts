import { Stream } from "./stream";

const NAME = "map";

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED, ERROR>): Stream.Transform<INPUT_STREAM, NAME, Stream<MAPPED, ERROR, NAME>> {
  return (inputStream, name) => {
    return Stream.transformer(
      new Stream<MAPPED, ERROR, NAME>(name ?? (NAME as NAME), async function* () {
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
  export type Mapper<VALUE, MAPPED, ERROR> = (
    value: VALUE,
  ) => MAPPED | Stream.Error<ERROR> | Promise<MAPPED | Stream.Error<ERROR>>;
}

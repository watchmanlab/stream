import { Stream } from "./stream";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
> extends Stream.Transformer<INPUT_STREAM, MAPPED, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const value of inputStream) {
        yield mapper(value);
      }
    });
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => {
    return new Map(name, inputStream, mapper);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (
    value: VALUE,
  ) => MAPPED | Stream.Error<ERROR> | Promise<MAPPED | Stream.Error<ERROR>>;
}

const stream = new Stream([1, 2, 3])
  .pipe(
    "map1",
    map((v) => (v > 3 ? v.toFixed() : new Stream.Error("kechmahaja"))),
  )
  .pipe(
    "map2",
    map((v) => v),
  )
  .pipe(
    "map3",
    map((v) => v),
  );

const v = stream.traversal.map2;

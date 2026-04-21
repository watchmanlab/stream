import { Stream } from "./stream";

const NAME = "map";

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Stream.Transform<INPUT_STREAM, NAME, Stream<MAPPED, NAME>> {
  return (inputStream, name) => {
    const mapped = new Stream<MAPPED, NAME>(name ?? (NAME as NAME));
    inputStream.listen((value) => {
      if (!mapped.listenersCount) return;
      mapped.push(mapper(value));
    });
    return Stream.transformer(mapped, inputStream);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}

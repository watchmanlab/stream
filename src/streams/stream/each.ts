import { Stream } from "./stream";

const NAME = "map";

class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
> extends Stream<MAPPED, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED>) {
    super(name);

    inputStream.listen(
      (value) => {
        const result = mapper(value);
        this.push(result);
      },
      { startSignal: this.firstListenerAdded, stopSignal: this.lastListenerRemoved },
    );
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(mapper: map.Mapper<VALUE, MAPPED>): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, NAME>> {
  return (inputStream, name) => Stream.transformer(new Map(name, inputStream, mapper), inputStream);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}

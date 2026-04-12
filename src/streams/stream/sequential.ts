import { Stream } from "./stream9";

const NAME = "sequential";

export function sequential<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
>(mapper: sequential.Mapper<VALUE, MAPPED, ERROR>): Stream.Transform<INPUT_STREAM, Stream<INPUT_STREAM>> {
  return (inputStream) => Stream.traversable();
}

export namespace sequential {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR> = (
    value: VALUE,
  ) => MAPPED | Stream.Err<ERROR> | Promise<MAPPED | Stream.Err<ERROR>>;
}

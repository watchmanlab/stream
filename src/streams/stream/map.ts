import { Stream } from "./stream9";

const NAME = "map";

class Map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  constructor(name: NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>) {
    super(name);

    inputStream.listen(async (value) => {
      const maybePromise = mapper(value);
      const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
      if (Stream.isErr(result)) {
        this.push(Stream.sourceErr({ value: value, error: result.value, source: this }) as never);
      } else {
        this.push(result);
      }
    });
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
>;
export function map<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
>(
  name: NAME,
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  nameOrMapper: NAME | map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  mapper?: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrMapper === "string"
        ? new Map(nameOrMapper, inputStream, mapper!)
        : new Map(NAME as NAME, inputStream, nameOrMapper),
      inputStream,
    );
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<CLEAN_VALUE, MAPPED, ERROR> = (
    value: CLEAN_VALUE,
  ) => MAPPED | Stream.Err<ERROR> | Promise<MAPPED | Stream.Err<ERROR>>;
}

const stream = new Stream<number>().pipe(map((v) => v.toFixed()));

stream.listen((v) => console.log(v));

stream.stream.push(1, 2, 3, 4);

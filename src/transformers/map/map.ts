import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
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
  protected _expectedErrors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}ExpectedErrors`>;
  protected _unexpectedErrors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, unknown>, `${NAME}UnexpectedErrors`>;
  constructor(name: NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = mapper(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._expectedErrors?.push({ value, error: result.value });
            yield Stream.sourceErr({ value, error: result.value, source: self }) as never;
            continue;
          }

          yield result as never;
        } catch (error) {
          self._unexpectedErrors?.push({ value, error: error });
          yield Stream.sourceErr({ value, error: error, source: self }) as never;
        }
      }
    });

    const self = this;
  }
  get errors(): {
    expected: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}ExpectedErrors`>;
    unexpected: Stream<Stream.ErrorEvent<CLEAN_VALUE, unknown>, `${NAME}UnexpectedErrors`>;
  } {
    const self = this;
    return {
      get expected() {
        if (!self._expectedErrors) self._expectedErrors = new Stream(`${self._name}ExpectedErrors` as never);
        return self._expectedErrors;
      },
      get unexpected() {
        if (!self._unexpectedErrors) self._unexpectedErrors = new Stream(`${self._name}UnexpectedErrors` as never);
        return self._unexpectedErrors;
      },
    };
  }
}

export function map<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
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
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, map.Name>, INPUT_STREAM>
>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapperOrName: map.Mapper<CLEAN_VALUE, MAPPED, ERROR> | NAME,
  mapper?: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof mapperOrName === "string"
        ? new Map(mapperOrName, inputStream, mapper!)
        : new Map(NAME as NAME, inputStream, mapperOrName),
      inputStream,
    );
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, ERROR> = (
    value: CLEAN_VALUE,
  ) =>
    | MAPPED
    | Stream.Err<ERROR>
    | Stream.Terminate
    | Stream.Skip
    | Promise<MAPPED | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip>;
}

const custom = <INPUT_STREAM extends Stream.AnyStream<number, "map2">>(source: INPUT_STREAM) =>
  source.pipe(map("inner1", (v) => v.toFixed())).pipe(map("inner2", (v) => true));

const stream = new Stream([1, 2, 4])
  .pipe(map("map1", (v) => v.toFixed()))
  .pipe(map("map2", (v) => Number(v)))
  .pipe(custom)
  .pipe(map("map3", (v) => v));

stream.inner2.inner1.map2.map1.name;
stream.inner2.inner1.map2.map1.root.name;

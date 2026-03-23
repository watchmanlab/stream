import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
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
      [SELF] extends [never]
        ? Stream.Transformer<
            Map<INPUT_STREAM, INPUT_NAME, never, CLEAN_VALUE, MAPPED, ERROR, NAME>,
            INPUT_NAME,
            INPUT_STREAM
          >
        : SELF
    >,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<CLEAN_VALUE, ERROR, Stream.Transformer<this, INPUT_NAME, INPUT_STREAM>>,
    `${NAME}Errors`
  >;

  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      mapper: map.Mapper<
        CLEAN_VALUE,
        MAPPED,
        ERROR,
        Stream.Transformer<
          Map<INPUT_STREAM, INPUT_NAME, never, CLEAN_VALUE, MAPPED, ERROR, NAME>,
          INPUT_NAME,
          INPUT_STREAM
        >
      >;
    },
  ) {
    const { name = NAME as NAME, inputStream, mapper } = options;
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;
        try {
          const maybePromise = mapper(cleanValue, self as never);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({
              type: "expected",
              value: cleanValue,
              error: result.value,
              source: self as never,
            });

            yield Stream.sourceErr({
              value: cleanValue,
              error: result.value,
              source: self,
            }) as never;

            continue;
          }

          yield result as never;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value: cleanValue, error: error.value, source: self as never });
            yield Stream.sourceErr({ value: value, error: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", value: cleanValue, error: error, source: self as never });
            yield Stream.sourceErr({ value: value, error: error, source: self }) as never;
          }
        }
      }
    });

    const self = this;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}

export function map<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<
    CLEAN_VALUE,
    MAPPED,
    ERROR,
    Stream.Transformer<Map<INPUT_STREAM, INPUT_NAME, never, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, MAPPED, ERROR, NAME>> {
  return (options) => new Map({ ...options, mapper });
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<CLEAN_VALUE, MAPPED, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) =>
    | MAPPED
    | Stream.Err<ERROR>
    | Stream.Terminate
    | Stream.Skip
    | Promise<MAPPED | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip>;
}

const stream = new Stream([1, 2, 4])
  .pipe(
    "mappa",
    map((v) => v.toFixed()),
  )
  .pipe(
    "justAMap",
    map((v) => v),
    //   ^?
  );

const s2 = stream.pipe(
  "toNumber",
  map((v) => Number(v)),
);

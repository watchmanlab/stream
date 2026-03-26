import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
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
      Stream.Traversable<Map<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}Errors`>;

  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>;
    },
  ) {
    super(options.name ?? (NAME as NAME), async function* () {
      for await (const value of options.inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = options.mapper(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", value, error: result.value });
            yield Stream.sourceErr({ value, error: result.value, source: self }) as never;
            continue;
          }

          yield result as never;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value, error: error.value });
            yield Stream.sourceErr({ value, error: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", value, error: error });
            yield Stream.sourceErr({ value, error: error, source: self }) as never;
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
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  NAME,
  Stream.Traversable<Map<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
> {
  return (options) => new Map({ ...options, mapper });
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

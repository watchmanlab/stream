import { Stream } from "../../streams/index.ts";

const NAME = "map";

export class Map<
  INPUT_STREAM extends Stream<any, any>,
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
      [SELF] extends [never] ? Map<INPUT_STREAM, never, CLEAN_VALUE, MAPPED, ERROR, NAME> : SELF
    >,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<CLEAN_VALUE, ERROR, Stream.Transformer<this, INPUT_STREAM>>,
    `${NAME}Errors`
  >;

  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR, Map<INPUT_STREAM, SELF, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;
        try {
          const maybePromise = mapper(cleanValue, self);
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
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR, Map<INPUT_STREAM, SELF, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
): Stream.Transforme<INPUT_STREAM, NAME, Map<INPUT_STREAM, SELF, CLEAN_VALUE, MAPPED, ERROR, NAME>> {
  return (_, imputStream, name) => new Map(name, imputStream, mapper);
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
  .pipe(map((v) => v));
//           ^?
stream.mappa;

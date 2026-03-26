import { Stream } from "../../streams/index.ts";
import { catchError } from "../catch-error/catch-error.ts";
import { each } from "../each/each.ts";
import { map, Map } from "../map/map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "filter";

export class Filter<
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
> extends Stream<
  | FILTERED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Filter<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, FILTERED, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}Errors`>;
  protected _events?: Stream<filter.Event<CLEAN_VALUE>, `${NAME}Events`>;
  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      predicate: filter.Predicate<CLEAN_VALUE, ERROR>;
    },
  ) {
    super(options.name ?? (NAME as NAME), async function* () {
      for await (const value of options.inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as FILTERED;
        try {
          const maybePromise = options.predicate(cleanValue);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({
              type: "expected",
              value: cleanValue,
              error: result.value,
            });

            yield Stream.sourceErr({
              value: cleanValue,
              error: result.value,
              source: self,
            }) as never;

            continue;
          }

          if (result) {
            yield cleanValue;
          } else {
            self._events?.push({ type: "filtered", value: cleanValue });
          }
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value: cleanValue, error: error.value });
            yield Stream.sourceErr({ value: value, error: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", value: cleanValue, error: error });
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
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
}

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.GardPredicate<CLEAN_VALUE, FILTERED>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, FILTERED, ERROR, NAME>>;

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>;

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Filter({ ...options, predicate });
}

export namespace filter {
  export type Name = typeof NAME;
  export type GardPredicate<CLEAN_VALUE, FILTERED extends CLEAN_VALUE> = (value: CLEAN_VALUE) => value is FILTERED;
  export type Predicate<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;

  export type Event<CLEAN_VALUE> = {
    type: "filtered";
    value: CLEAN_VALUE;
  };
}

const stream = new Stream([1, 2, 3])
  .pipe(
    "SSS",
    filter((v) => {
      if (v === 1) return Stream.err("kechmahaja" as const);
      return v !== 2;
    }),
  )
  .pipe(
    map((v) => {
      if (v === 3) return Stream.err("error map" as const);
      return v.toFixed();
    }),
  )
  .pipe(each((v) => console.log(v)))
  .pipe(
    catchError((e) => {
      if (e.sourceName === "SSS") {
        e.source;
      } else {
        e.error;
      }
    }),
  )
  .pipe(pump());

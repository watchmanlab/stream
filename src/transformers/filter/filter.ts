import { Stream } from "../../streams/index.ts";
import { catchError } from "../catch-error/catch-error.ts";
import { each } from "../each/each.ts";
import { map } from "../map/map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "filter";

export class Filter<
  INPUT_STREAM extends Stream.AnyStream,
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
      Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, FILTERED, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  protected _filtered?: Stream<CLEAN_VALUE, `${NAME}Filtered`>;
  protected _expectedErrors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}ExpectedErrors`>;
  protected _unexpectedErrors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, unknown>, `${NAME}UnexpectedErrors`>;

  constructor(name: NAME, inputStream: INPUT_STREAM, predicate: filter.Predicate<CLEAN_VALUE, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = predicate(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._expectedErrors?.push({ value, error: result.value });
            yield Stream.sourceErr({ value, error: result.value, source: self }) as never;
            continue;
          }

          if (result) {
            yield value;
          } else {
            self._filtered?.push(value);
          }
        } catch (error) {
          self._unexpectedErrors?.push({ value, error: error });
          yield Stream.sourceErr({ value, error: error, source: self }) as never;
        }
      }
    });
    const self = this;
  }
  get filtered() {
    if (!this._filtered) this._filtered = new Stream(`${this._name}Filtered` as never);
    return this._filtered;
  }
  get expectedErrors() {
    if (!this._expectedErrors) this._expectedErrors = new Stream(`${this._name}ExpectedErrors` as never);
    return this._expectedErrors;
  }
  get unexpectedErrors() {
    if (!this._unexpectedErrors) this._unexpectedErrors = new Stream(`${this._name}UnexpectedErrors` as never);
    return this._unexpectedErrors;
  }
}

export function filter<
  NAME extends string,
  INPUT_STREAM extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
>(
  name: NAME,
  predicate: filter.GardPredicate<CLEAN_VALUE, FILTERED>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, FILTERED, ERROR, NAME>, INPUT_STREAM>
>;
export function filter<
  NAME extends string,
  INPUT_STREAM extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
  predicate: filter.Predicate<CLEAN_VALUE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
>;
export function filter<
  INPUT_STREAM extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
>(
  predicate: filter.GardPredicate<CLEAN_VALUE, FILTERED>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, FILTERED, ERROR, filter.Name>, INPUT_STREAM>
>;
export function filter<
  INPUT_STREAM extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  predicate: filter.Predicate<CLEAN_VALUE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, filter.Name>, INPUT_STREAM>
>;
export function filter<
  INPUT_STREAM extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  nameOrPredicate: NAME | filter.Predicate<CLEAN_VALUE, ERROR>,
  predicate?: filter.Predicate<CLEAN_VALUE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Filter<INPUT_STREAM, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrPredicate === "string"
        ? new Filter(nameOrPredicate, inputStream, predicate!)
        : new Filter(NAME as NAME, inputStream, nameOrPredicate),
      inputStream,
    );
}

export namespace filter {
  export type Name = typeof NAME;
  export type GardPredicate<CLEAN_VALUE, FILTERED extends CLEAN_VALUE> = (value: CLEAN_VALUE) => value is FILTERED;
  export type Predicate<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) =>
    | boolean
    | Stream.Terminate
    | Stream.Skip
    | Stream.Err<ERROR>
    | Promise<boolean | Stream.Terminate | Stream.Skip | Stream.Err<ERROR>>;

  export type Event<CLEAN_VALUE> = {
    type: "filtered";
    value: CLEAN_VALUE;
  };
}

const stream = new Stream([1, 2, 3])
  .pipe(
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

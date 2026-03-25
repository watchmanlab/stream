import { Stream } from "../../streams/index.ts";
import { catchError } from "../catch-error/catch-error.ts";
import { each } from "../each/each.ts";
import { map, Map } from "../map/map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "filter";

export class Filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
> extends Map<
  INPUT_STREAM,
  INPUT_NAME,
  [SELF] extends [never] ? Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME> : SELF,
  CLEAN_VALUE,
  FILTERED,
  ERROR,
  NAME
> {
  protected _events?: Stream<
    filter.Event<CLEAN_VALUE, Stream.Transformer<this, INPUT_NAME, INPUT_STREAM>>,
    `${NAME}Events`
  >;
  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      predicate: filter.Predicate<
        CLEAN_VALUE,
        ERROR,
        Stream.Transformer<
          Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME>,
          INPUT_NAME,
          INPUT_STREAM
        >
      >;
    },
  ) {
    const { name = NAME as NAME, inputStream, predicate } = options;
    super({
      inputStream,
      name,
      token: options.token,
      mapper: async (value) => {
        const maybePromise = predicate(value, self as never);
        const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
        if (Stream.isErr(result)) return result as FILTERED;
        if (result) return value as FILTERED;
        self._events?.push({ type: "filtered", value: result as FILTERED, self: self as never });
        return Stream.SKIP;
      },
    });
    const self = this;
  }

  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
}

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.GardPredicate<
    CLEAN_VALUE,
    FILTERED,
    Stream.Transformer<
      Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>,
      INPUT_NAME,
      INPUT_STREAM
    >
  >,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME>>;

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<
    CLEAN_VALUE,
    ERROR,
    Stream.Transformer<
      Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>,
      INPUT_NAME,
      INPUT_STREAM
    >
  >,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>;

export function filter<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<
    CLEAN_VALUE,
    ERROR,
    Stream.Transformer<
      Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>,
      INPUT_NAME,
      INPUT_STREAM
    >
  >,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Filter({ ...options, predicate });
}

export namespace filter {
  export type Name = typeof NAME;
  export type GardPredicate<
    CLEAN_VALUE,
    FILTERED extends CLEAN_VALUE,
    SELF extends Stream.Transformer<Stream<any, any>, string, Stream<any, any>>,
  > = (value: CLEAN_VALUE, self: SELF) => value is FILTERED;
  export type Predicate<
    CLEAN_VALUE,
    ERROR,
    SELF extends Stream.Transformer<Stream<any, any>, string, Stream<any, any>>,
  > = (value: CLEAN_VALUE, self: SELF) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;

  export type Event<CLEAN_VALUE, SELF extends Stream.Transformer<any, any, any>> = {
    type: "filtered";
    value: CLEAN_VALUE;
    self: SELF;
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
        e.source.errors.name;
      } else {
        e.error;
      }
    }),
  )
  .pipe(pump());

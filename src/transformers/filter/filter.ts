import { Stream } from "../../streams/index.ts";
import { catchError } from "../catch-error/catch-error.ts";
import { each } from "../each/each.ts";
import { map, Map } from "../map/map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "filter";

export class Filter<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
>
  extends Map<
    SOURCE,
    [SELF] extends [never] ? Filter<SOURCE, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME> : SELF,
    CLEAN_VALUE,
    FILTERED,
    ERROR,
    NAME
  >
  implements Record<any, any>
{
  protected _events?: Stream<filter.Event<CLEAN_VALUE, this>, `${NAME}Events`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    predicate: filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME>>,
  ) {
    super(source, name, async (value) => {
      const maybePromise = predicate(value, self);
      const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
      if (Stream.isErr(result)) return result as FILTERED;
      if (result) return value as FILTERED;
      self._events?.push({ type: "filtered", value: result as FILTERED, self });
      return Stream.SKIP;
    });
    const self = this;
  }

  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
}

export function filter<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.GardPredicate<CLEAN_VALUE, FILTERED, Filter<SOURCE, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, SELF, CLEAN_VALUE, FILTERED, ERROR, NAME>>;

export function filter<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>;

export function filter<
  SOURCE extends Stream<any, any>,
  SELF extends Stream<any, any> = never,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, SELF, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>> {
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace filter {
  export type Name = typeof NAME;
  export type GardPredicate<CLEAN_VALUE, FILTERED extends CLEAN_VALUE, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => value is FILTERED;
  export type Predicate<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;

  export type Event<CLEAN_VALUE, SELF extends Stream<any, any>> = { type: "filtered"; value: CLEAN_VALUE; self: SELF };
}

new Stream([1, 2, 3])
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
      switch (e.source.name) {
        case "filter":
          e.source;

        case "map":
          e.source;
      }
    }),
  )
  .pipe(pump());

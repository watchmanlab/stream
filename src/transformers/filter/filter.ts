import { Stream } from "../../streams/";

const NAME = "filter";

export class Filter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = Filter.Name,
> extends Stream<
  | FILTERED
  | Stream.ExtractSentinel<SOURCE>
  | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, Filter<SOURCE, CLEAN_VALUE, FILTERED, ERROR, NAME>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    predicate: Filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, CLEAN_VALUE, FILTERED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as FILTERED;
        try {
          const maybePromise = predicate(cleanValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: result.value });

            yield Stream.sourceErr({
              value: value,
              detail: result.value,
              source: self,
            }) as never;

            continue;
          }

          if (result) yield cleanValue;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: error.value });
            yield Stream.sourceErr({ value: value, detail: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", source: self, value: cleanValue, detail: error });
            yield Stream.sourceErr({ value: value, detail: error, source: self }) as never;
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

export function filter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  FILTERED extends CLEAN_VALUE = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = Filter.Name,
>(
  predicate: Filter.GardPredicate<CLEAN_VALUE, FILTERED, Filter<SOURCE, CLEAN_VALUE, FILTERED, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, CLEAN_VALUE, FILTERED, ERROR, NAME>>;

export function filter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = Filter.Name,
>(
  predicate: Filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>;

export function filter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  ERROR = never,
  NAME extends string = Filter.Name,
>(
  predicate: Filter.Predicate<CLEAN_VALUE, ERROR, Filter<SOURCE, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, CLEAN_VALUE, CLEAN_VALUE, ERROR, NAME>> {
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace Filter {
  export type Name = typeof NAME;
  export type GardPredicate<CLEAN_VALUE, FILTERED extends CLEAN_VALUE, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => value is FILTERED;
  export type Predicate<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;
}

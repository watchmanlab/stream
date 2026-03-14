import { Stream } from "../../streams/";

const NAME = "filter";

export class Filter<
  SOURCE extends Stream<any, any>,
  FILTERED extends Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>> =
    Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>>,
  ERROR = never,
  NAME extends string = Filter.Name,
> extends Stream<
  | FILTERED
  | Stream.ExtractErrorFromValue<Stream.ExtractValueFromSource<SOURCE>>
  | Stream.MaybeSourceErr<
      ERROR,
      Stream.SourceErr<
        Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>>,
        ERROR,
        Filter<Stream.ExtractValueFromSource<SOURCE>, FILTERED, ERROR, NAME>
      >
    >,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>>, ERROR, this>,
    `${NAME}Errors`
  >;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    predicate: Filter.Predicate<
      Stream.ExtractValueFromSource<SOURCE>,
      ERROR,
      Filter<Stream.ExtractValueFromSource<SOURCE>, FILTERED, ERROR, NAME>
    >,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value as never;
          continue;
        }

        const rawValue = value as FILTERED;
        try {
          const maybePromise = predicate(rawValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", source: self, value: rawValue, detail: result.value });

            yield Stream.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as never;

            continue;
          }

          if (result) yield rawValue;
        } catch (error) {
          self._errors?.push({ type: "unexpected", source: self, value: rawValue, detail: error });

          yield Stream.sourceErr({
            source: self,
            value: value,
            detail: error,
          }) as never;
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
  FILTERED extends Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>> =
    Stream.ExtractCleanValueFromValue<Stream.ExtractValueFromSource<SOURCE>>,
  ERROR = never,
  NAME extends string = Filter.Name,
>(
  predicate: Filter.GardPredicate<
    Stream.ExtractValueFromSource<SOURCE>,
    FILTERED,
    Filter<SOURCE, FILTERED, ERROR, NAME>
  >,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, FILTERED, ERROR, NAME>>;

export function filter<SOURCE extends Stream<any, any>, ERROR = never, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<
    Stream.ExtractValueFromSource<SOURCE>,
    ERROR,
    Filter<SOURCE, Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, NAME>
  >,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, NAME>>;

export function filter<SOURCE extends Stream<any, any>, ERROR = never, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<
    Stream.ExtractValueFromSource<SOURCE>,
    ERROR,
    Filter<SOURCE, Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, NAME>
  >,
): Stream.Transformer<NAME, SOURCE, Filter<SOURCE, Stream.ExtractCleanValueFromSource<SOURCE>, ERROR, NAME>> {
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace Filter {
  export type Name = typeof NAME;
  export type GardPredicate<
    VALUE,
    FILTERED extends Stream.ExtractCleanValueFromValue<VALUE>,
    SELF extends Stream<any, any>,
  > = (value: Stream.ExtractCleanValueFromValue<VALUE>, self: SELF) => value is FILTERED;
  export type Predicate<VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.ExtractCleanValueFromValue<VALUE>,
    self: SELF,
  ) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;
}

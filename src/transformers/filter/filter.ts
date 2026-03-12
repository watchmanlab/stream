import { Stream } from "../../streams/";
import { each } from "../each";
import { map } from "../map";
import { pump } from "../pump";

const NAME = "filter";

export class Filter<
  VALUE,
  FILTERED extends Stream.SafeValueOf<VALUE> = Stream.SafeValueOf<VALUE>,
  ERROR = never,
  NAME extends string = Filter.Name,
> extends Stream<
  | FILTERED
  | Stream.SourceErrOf<VALUE>
  | Stream.MaybeSourceErr<
      ERROR,
      Stream.SourceErr<Stream.SafeValueOf<VALUE>, ERROR, Filter<VALUE, FILTERED, ERROR, NAME>>
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<Stream.SafeValueOf<VALUE>, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, FILTERED, ERROR, NAME>>,
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
  VALUE,
  FILTERED extends Stream.SafeValueOf<VALUE> = Stream.SafeValueOf<VALUE>,
  ERROR = never,
  NAME extends string = Filter.Name,
>(
  predicate: Filter.GardPredicate<VALUE, FILTERED, Filter<VALUE, FILTERED, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, FILTERED, ERROR, NAME>>;

export function filter<VALUE, ERROR = never, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, Stream.SafeValueOf<VALUE>, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, Stream.SafeValueOf<VALUE>, ERROR, NAME>>;

export function filter<VALUE, ERROR = never, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, Stream.SafeValueOf<VALUE>, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, Stream.SafeValueOf<VALUE>, ERROR, NAME>> {
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace Filter {
  export type Name = typeof NAME;
  export type GardPredicate<VALUE, FILTERED extends Stream.SafeValueOf<VALUE>, SELF extends Stream<any, any>> = (
    value: Stream.SafeValueOf<VALUE>,
    self: SELF,
  ) => value is FILTERED;
  export type Predicate<VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: Stream.SafeValueOf<VALUE>,
    self: SELF,
  ) => boolean | Stream.Err<ERROR> | Promise<boolean | Stream.Err<ERROR>>;
}

const stream = new Stream<{ type: "1"; name: "ch" } | { type: "2"; age: 49 }>([
  { type: "1", name: "ch" },
  { type: "2", age: 49 },
])
  .pipe(filter((v) => v.type === "1"))
  .pipe(map((v) => v.name))
  .pipe(
    each((v) => {
      console.log(v);
      if (!v) return Stream.err("kechmahaja");
    }),
  )
  .pipe(pump());

stream.each.map.filter.root.push({ type: "2", age: 49 });
stream.each.map.filter.root.push({ type: "1", name: "ch" });

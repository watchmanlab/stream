import { Stream } from "../../streams/";
import { map, Map } from "../map";

const NAME = "filter";

export class Filter<
  VALUE,
  FILTERED extends VALUE = VALUE,
  ERROR = unknown,
  NAME extends string = Filter.Name,
> extends Stream<FILTERED, NAME> {
  private _map: Map<VALUE, [FILTERED, boolean], ERROR>;
  protected _errors?: Stream<map.ErrorEvent<ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, FILTERED, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._map[Symbol.asyncIterator]();
      let next = await generator.next();
      try {
        while (!next.done) {
          const [value, keep] = next.value;
          if (keep) {
            next = await generator.next();
            continue;
          }
          const feedback = yield value;
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;
    this._map = new Map<VALUE, [FILTERED, boolean], ERROR>(source, undefined, async (value, _, compensate) => {
      const result = await predicate(value, this, compensate);
      if (Stream.Result.isErr(result)) return result;
      if (result) {
        return [value as FILTERED, true];
      } else {
        return [value as FILTERED, false];
      }
    });
  }

  get errors() {
    if (!this._errors)
      this._errors = this._map.errors.pipe(
        `${this._name}Errors`,
        map((error) => ({ ...error, self: this })),
      );

    return this._errors;
  }
}

export function filter<VALUE, FILTERED extends VALUE = VALUE, ERROR = unknown, NAME extends string = Filter.Name>(
  predicate: Filter.GardPredicate<VALUE, FILTERED, Filter<VALUE, FILTERED, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, FILTERED, ERROR, NAME>>;

export function filter<VALUE, ERROR, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, ERROR, NAME>>;

export function filter<VALUE, ERROR, NAME extends string = Filter.Name>(
  predicate: Filter.Predicate<VALUE, ERROR, Filter<VALUE, VALUE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Filter<VALUE, VALUE, ERROR, NAME>> {
  return (_, source, name) => new Filter(source, name, predicate);
}

export namespace Filter {
  export type Name = typeof NAME;
  export type GardPredicate<VALUE, FILTERED extends VALUE, SELF extends Stream<VALUE, any>> = (
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => value is FILTERED;
  export type Predicate<VALUE, ERROR, SELF extends Stream<VALUE, any>> = (
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => boolean | Stream.Result.Err<ERROR> | Promise<boolean | Stream.Result.Err<ERROR>>;
}

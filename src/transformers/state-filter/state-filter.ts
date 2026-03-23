import { CallExpression } from "typescript";
import { Stream } from "../../streams";

const NAME = "stateFilter";

export class StateFilter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = stateFilter.Name,
> extends Stream<
  | Stream.ExtractValue<SOURCE>
  | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, StateFilter<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    initialState: STATE,
    predicate: stateFilter.Predicate<CLEAN_VALUE, STATE, ERROR, StateFilter<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      let state = initialState;

      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }
        const cleanValue = value as CLEAN_VALUE;
        try {
          const maybePromise = predicate(state, cleanValue, self);
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
          const [ok, newState] = result;

          state = { ...state, ...newState };

          if (ok) yield cleanValue;
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

export function stateFilter<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = stateFilter.Name,
>(
  initialState: STATE,
  predicate: stateFilter.Predicate<CLEAN_VALUE, STATE, ERROR, StateFilter<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
): Stream.Transform<NAME, SOURCE, StateFilter<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateFilter(source, name, initialState, predicate);
}

export namespace stateFilter {
  export type Name = typeof NAME;

  export type Predicate<CLEAN_VALUE, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<any, any>> = (
    state: STATE,
    value: CLEAN_VALUE,
    self: SELF,
  ) => [boolean, Partial<STATE>] | Stream.Err<ERROR> | Promise<[boolean, Partial<STATE>] | Stream.Err<ERROR>>;
}

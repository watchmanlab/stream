import { Stream } from "../../streams";
import { each } from "../each";
import { effect } from "../effect";
import { map } from "../map";
import { pump } from "../pump";
import { stateMap, StateMap } from "../state-map";

const NAME = "stateEach";

export class StateEach<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = stateEach.Name,
> extends Stream<Stream.ExtractValue<SOURCE>, NAME> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: SOURCE,
    name = NAME as NAME,
    initialState: STATE,
    callback: stateEach.Callback<CLEAN_VALUE, STATE, ERROR, StateEach<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      let state = initialState;

      for await (const value of source) {
        if (Stream.isSourceErr(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;

        try {
          const maybePromise = callback(state, cleanValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr<ERROR>(result)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: result.value });

            yield Stream.sourceErr({
              source: self,
              value: value,
              detail: result.value,
            }) as never;

            continue;
          }

          state = { ...state, ...result };

          yield cleanValue;
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

export function stateEach<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = stateEach.Name,
>(
  initialState: STATE,
  callback: stateEach.Callback<CLEAN_VALUE, STATE, ERROR, StateEach<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, StateEach<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateEach(source, name, initialState, callback);
}

export namespace stateEach {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<any, any>> = (
    state: STATE,
    value: CLEAN_VALUE,
    self: SELF,
  ) => Partial<STATE> | Stream.Err<ERROR> | Promise<Partial<STATE> | Stream.Err<ERROR>>;
}

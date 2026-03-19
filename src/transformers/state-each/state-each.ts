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
  private _stateMap: StateMap<SOURCE, CLEAN_VALUE, CLEAN_VALUE, STATE, ERROR>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    initialState: STATE,
    callback: stateEach.Callback<CLEAN_VALUE, STATE, ERROR, StateEach<SOURCE, CLEAN_VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      yield* self._stateMap;
    });
    const self = this;

    this._stateMap = new StateMap<SOURCE, CLEAN_VALUE, CLEAN_VALUE, STATE, ERROR>(
      source,
      undefined,
      initialState,
      async (state, value, _) => {
        const newState = await callback(state, value, this);
        if (Stream.isErr(newState)) return newState;
        return [value, newState];
      },
    );
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

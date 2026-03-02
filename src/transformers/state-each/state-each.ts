import { Stream } from "../../streams";
import { effect } from "../effect";
import { map } from "../map";
import { pump } from "../pump";
import { stateMap, StateMap } from "../state-map";

const NAME = "stateEach";

export class StateEach<
  VALUE,
  STATE extends Record<string, unknown>,
  ERROR,
  NAME extends string = stateEach.Name,
> extends Stream<VALUE, NAME> {
  private _stateMap: StateMap<VALUE, VALUE, STATE, ERROR>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    initialState: STATE,
    callback: stateEach.Callback<VALUE, STATE, ERROR, StateEach<VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._stateMap[Symbol.asyncIterator]();
      let next = await generator.next();
      try {
        while (!next.done) {
          const feedback = yield next.value;
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;

    this._stateMap = new StateMap<VALUE, VALUE, STATE, ERROR>(
      source,
      undefined,
      initialState,
      async (state, value, _, compensate) => {
        const newState = await callback(state, value, this, compensate);
        if (Stream.Result.isErr(newState)) return newState;
        return [value, newState];
      },
    );
  }
}

export function stateEach<VALUE, STATE extends Record<string, unknown>, ERROR, NAME extends string = stateEach.Name>(
  initialState: STATE,
  callback: stateEach.Callback<VALUE, STATE, ERROR, StateEach<VALUE, STATE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, StateEach<VALUE, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateEach(source, name, initialState, callback);
}

export namespace stateEach {
  export type Name = typeof NAME;

  export type Callback<VALUE, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<any, any>> = (
    state: STATE,
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => STATE | Stream.Result.Err<ERROR> | Promise<STATE | Stream.Result.Err<ERROR>>;
}

new Stream([1, 2, 3])
  .pipe(
    stateEach({ count: 0 }, (state, v) => {
      console.log(state);

      return { count: state.count + 4 };
    }),
  )
  .pipe(effect((v) => console.log(v)))
  .pipe(pump());

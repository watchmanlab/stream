import { Stream } from "../../streams/index.ts";
import { effect } from "../effect/effect.ts";
import { Map, map } from "../map/map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "stateMap";

export class StateMap<
  VALUE,
  MAPPED = VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = unknown,
  NAME extends string = stateMap.Name,
> extends Stream<MAPPED, NAME> {
  private _map: Map<VALUE, MAPPED, ERROR>;
  protected _errors?: Stream<map.ErrorEvent<ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    initialState: STATE,
    mapper: stateMap.Mapper<VALUE, MAPPED, STATE, ERROR, StateMap<VALUE, MAPPED, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._map[Symbol.asyncIterator]();
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

    let state = initialState;
    this._map = new Map<VALUE, MAPPED, ERROR>(source, undefined, async (value, _, compensate) => {
      const result = await mapper(state, value, this, compensate);
      if (Stream.Result.isErr(result)) return result;
      const [mapped, newState] = result;
      state = { ...state, ...newState };
      return mapped;
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

export function stateMap<
  VALUE,
  MAPPED = VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = unknown,
  NAME extends string = stateMap.Name,
>(
  initialState: STATE,
  mapper: stateMap.Mapper<VALUE, MAPPED, STATE, ERROR, StateMap<VALUE, MAPPED, STATE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, StateMap<VALUE, MAPPED, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateMap(source, name, initialState, mapper);
}

export namespace stateMap {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<MAPPED, any>> = (
    state: STATE,
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => [MAPPED, STATE] | Stream.Result.Err<ERROR> | Promise<[MAPPED, STATE] | Stream.Result.Err<ERROR>>;
}

new Stream([1, 2, 3])
  .pipe(
    stateMap({ count: 0 }, (state, v) => {
      return [state.count, { count: state.count + 4 }];
    }),
  )
  .pipe(effect((v) => console.log(v)))
  .pipe(pump());

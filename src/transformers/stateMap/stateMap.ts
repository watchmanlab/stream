import { Stream } from "../../streams/index.ts";
import { Map, map } from "../map/map.ts";

const NAME = "statefull";

export class StateMap<
  VALUE,
  MAPPED = VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = unknown,
  NAME extends string = stateMap.Name,
> extends Stream<MAPPED, NAME> {
  private _map: Map<VALUE, [MAPPED, STATE], ERROR>;
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
          const feedback = yield next.value[0];
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;

    let state = initialState;
    this._map = new Map<VALUE, [MAPPED, STATE], ERROR>(source, undefined, async (value, _, compensate) => {
      const [result, newState] = await mapper(state, value, this, compensate);
      if (Stream.Result.isErr(result)) return result;
      state = { ...state, ...newState };
      return [result, state];
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

export namespace stateMap {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<MAPPED, any>> = (
    state: STATE,
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => [MAPPED | Stream.Result.Err<ERROR>, STATE] | Promise<[MAPPED | Stream.Result.Err<ERROR>, STATE]>;
}

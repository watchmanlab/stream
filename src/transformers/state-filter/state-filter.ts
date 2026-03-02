import { Stream } from "../../streams";
import { map } from "../map";
import { StateMap } from "../state-map";

const NAME = "stateFilter";

export class StateFilter<
  VALUE,
  STATE extends Record<string, unknown>,
  ERROR,
  NAME extends string = stateFilter.Name,
> extends Stream<VALUE, NAME> {
  protected _stateMap: StateMap<VALUE, { value: VALUE; keep: boolean }, STATE, ERROR>;
  protected _errors?: Stream<map.ErrorEvent<ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    initialState: STATE,
    predicate: stateFilter.Predicate<VALUE, STATE, ERROR, StateFilter<VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._stateMap[Symbol.asyncIterator]();
      let next = await generator.next();
      try {
        while (!next.done) {
          const { value, keep } = next.value;

          if (!keep) {
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

    this._stateMap = new StateMap<VALUE, { value: VALUE; keep: boolean }, STATE, ERROR>(
      source,
      undefined,
      initialState,
      async (state, value, _, compensate) => {
        const result = await predicate(state, value, this, compensate);
        if (Stream.Result.isErr(result)) return result;
        const [keep, newState] = result;
        return [{ value, keep }, newState];
      },
    );
  }

  get errors() {
    if (!this._errors)
      this._errors = this._stateMap.errors.pipe(
        `${this._name}Errors`,
        map((error) => ({ ...error, self: this })),
      );
    return this._errors;
  }
}

export function stateFilter<
  VALUE,
  STATE extends Record<string, unknown>,
  ERROR,
  NAME extends string = stateFilter.Name,
>(
  initialState: STATE,
  predicate: stateFilter.Predicate<VALUE, STATE, ERROR, StateFilter<VALUE, STATE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, StateFilter<VALUE, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateFilter(source, name, initialState, predicate);
}

export namespace stateFilter {
  export type Name = typeof NAME;

  export type Predicate<VALUE, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<VALUE, any>> = (
    state: STATE,
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => [boolean, STATE] | Stream.Result.Err<ERROR> | Promise<[boolean, STATE] | Stream.Result.Err<ERROR>>;
}

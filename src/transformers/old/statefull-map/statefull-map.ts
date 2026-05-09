import { Stream } from "../../../streams/index.ts";

const NAME = "statefulMap";

class StatefulMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulMap.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<StatefulMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  private _state: STATE;
  private _stateChanged?: Stream<{ value?: CLEAN_VALUE; state: Partial<STATE> }, `${NAME}StateChanged`>;

  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    initialState: STATE,
    mapper: statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
  ) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = mapper(self.state, value);
          const [mapped, newState] = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          self.setState(newState, value);

          if (Stream.isGenericError(mapped)) {
            yield Stream.sourceErr({
              value,
              error: mapped.value,
              source: self,
            }) as never;
            continue;
          }

          yield mapped as never;
        } catch (error) {
          yield Stream.sourceErr({ value, error, source: self }) as never;
        }
      }
    });
    const self = Stream.traversable(this, inputStream);
    this._state = initialState;
  }
  private setState(state: Partial<STATE>, value?: CLEAN_VALUE) {
    if (Object.keys(state).length > 0) {
      this._state = { ...this._state, ...state };
      this._stateChanged?.push({ value, state });
    }
  }
  get state(): STATE {
    return { ...this._state };
  }
  set state(state: Partial<STATE>) {
    this.setState(state);
  }
  get stateChanged() {
    if (!this._stateChanged) this._stateChanged = new Stream(`${this._name}StateChanged`);
    return this._stateChanged;
  }
}
export function statefulMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulMap.Name,
>(
  initialState: STATE,
  mapper: statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefulMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>>;
export function statefulMap<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
>(
  name: NAME,
  initialState: STATE,
  mapper: statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefulMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>>;
export function statefulMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulMap.Name,
>(
  nameOrInitialState: NAME | STATE,
  initialStateOrMapper: STATE | statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
  mapper?: statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefulMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrInitialState === "string"
        ? new StatefulMap(nameOrInitialState, inputStream, initialStateOrMapper as STATE, mapper!)
        : new StatefulMap(
            NAME as NAME,
            inputStream,
            nameOrInitialState,
            initialStateOrMapper as statefulMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
          ),
      inputStream,
    );
}

export namespace statefulMap {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) =>
    | [MAPPED | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]
    | Promise<[MAPPED | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]>;
}

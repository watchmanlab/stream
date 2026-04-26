import { Stream } from "../../streams/index.ts";

const NAME = "statefulEach";

export class StatefulEach<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulEach.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<StatefulEach<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  private _state: STATE;
  private _stateChanged?: Stream<{ value?: CLEAN_VALUE; state: Partial<STATE> }, `${NAME}StateChanged`>;
  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    initialState: STATE,
    callback: statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
  ) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }

        try {
          const maybePromise = callback(self.state, value);
          const [result, newState] = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          self.setState(newState, value);

          if (Stream.isGenericError<ERROR>(result)) {
            yield Stream.sourceErr({
              value: value,
              error: result.value,
              source: self,
            });

            continue;
          }
          if (Stream.isControl(result)) {
            yield result;
            continue;
          }

          yield value;
        } catch (error) {
          yield Stream.sourceErr({ value: value, error, source: self }) as never;
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
export function statefulEach<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulEach.Name,
>(
  initialState: STATE,
  callback: statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulEach<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefulEach<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
>(
  name: NAME,
  initialState: STATE,
  callback: statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulEach<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefulEach<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulEach.Name,
>(
  nameOrInitialState: NAME | STATE,
  initialStateOrCallback: STATE | statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
  callback?: statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulEach<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrInitialState === "string"
        ? new StatefulEach(nameOrInitialState, inputStream, initialStateOrCallback as STATE, callback!)
        : new StatefulEach(
            NAME as NAME,
            inputStream,
            nameOrInitialState,
            initialStateOrCallback as statefulEach.Callback<CLEAN_VALUE, STATE, ERROR>,
          ),
      inputStream,
    );
}

export namespace statefulEach {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) =>
    | [undefined | null | Stream.Error<ERROR> | Stream.Control, Partial<STATE>]
    | Promise<[undefined | null | Stream.Error<ERROR> | Stream.Control, Partial<STATE>]>;
}

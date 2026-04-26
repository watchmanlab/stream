import { Stream } from "../../streams/index.ts";

const NAME = "statefulFilter";

class StatefulFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulFilter.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<StatefulFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  private _state: STATE;
  private _stateChanged?: Stream<{ value?: CLEAN_VALUE; state: Partial<STATE> }, `${NAME}StateChanged`>;

  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    initialState: STATE,
    predicate: statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
  ) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }

        try {
          const maybePromise = predicate(self.state, value);
          const [ok, newState] = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          self.setState(newState, value);

          if (Stream.isGenericError(ok)) {
            yield Stream.sourceErr({
              value: value,
              error: ok.value,
              source: self,
            });

            continue;
          }

          if (Stream.isControl(ok)) {
            yield ok;
            continue;
          }

          if (ok) yield value;
        } catch (error) {
          yield Stream.sourceErr({ value: value, error: error, source: self }) as never;
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

export function statefulFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulFilter.Name,
>(
  initialState: STATE,
  predicate: statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefulFilter<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
>(
  name: NAME,
  initialState: STATE,
  predicate: statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefulFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefulFilter.Name,
>(
  nameOrInitialState: NAME | STATE,
  initialStateOrPredicate: STATE | statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
  predicate?: statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefulFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrInitialState === "string"
        ? new StatefulFilter(nameOrInitialState, inputStream, initialStateOrPredicate as STATE, predicate!)
        : new StatefulFilter(
            NAME as NAME,
            inputStream,
            nameOrInitialState,
            initialStateOrPredicate as statefulFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
          ),
      inputStream,
    );
}

export namespace statefulFilter {
  export type Name = typeof NAME;

  export type Predicate<CLEAN_VALUE, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) =>
    | [boolean | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]
    | Promise<[boolean | Stream.Error<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]>;
}

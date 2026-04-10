import { Stream } from "../../streams";

const NAME = "statefullFilter";

class StatefullFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullFilter.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<StatefullFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    initialState: STATE,
    predicate: statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
  ) {
    super(name, async function* () {
      let state = initialState;

      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }
        const cleanValue = value as CLEAN_VALUE;
        try {
          const maybePromise = predicate(state, cleanValue);
          const [ok, newState] = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (state !== newState) state = { ...state, ...newState };

          if (Stream.isErr(ok)) {
            yield Stream.sourceErr({
              value: value,
              error: ok.value,
              source: self,
            }) as never;

            continue;
          }

          if (Stream.isControl(ok)) {
            yield ok as never;
            continue;
          }

          if (ok) yield cleanValue as never;
        } catch (error) {
          yield Stream.sourceErr({ value: value, error: error, source: self }) as never;
        }
      }
    });

    const self = Stream.traversable(this, inputStream);
  }
}

export function statefullFilter<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
>(
  name: NAME,
  initialState: STATE,
  predicate: statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefullFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefullFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullFilter.Name,
>(
  initialState: STATE,
  predicate: statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefullFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
>;
export function statefullFilter<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullFilter.Name,
>(
  nameOrInitialState: NAME | STATE,
  initialStateOrPredicate: STATE | statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
  predicate?: statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<StatefullFilter<INPUT_STREAM, CLEAN_VALUE, STATE, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrInitialState === "string"
        ? new StatefullFilter(nameOrInitialState, inputStream, initialStateOrPredicate as STATE, predicate!)
        : new StatefullFilter(
            NAME as NAME,
            inputStream,
            nameOrInitialState,
            initialStateOrPredicate as statefullFilter.Predicate<CLEAN_VALUE, STATE, ERROR>,
          ),
      inputStream,
    );
}

export namespace statefullFilter {
  export type Name = typeof NAME;

  export type Predicate<CLEAN_VALUE, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) =>
    | [boolean | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]
    | Promise<[boolean | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]>;
}

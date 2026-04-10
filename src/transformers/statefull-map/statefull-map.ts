import { Stream } from "../../streams/index.ts";
import { each } from "../each/each.ts";
import { pump } from "../pump/pump.ts";

const NAME = "statefullMap";

class StatefullMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullMap.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<StatefullMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    initialState: STATE,
    mapper: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
  ) {
    super(name, async function* () {
      let state = initialState;
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = mapper(state, value);
          const [mapped, newState] = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (state !== newState) state = { ...state, ...newState };

          if (Stream.isErr(mapped)) {
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
  }
}
export function statefullMap<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
>(
  name: NAME,
  initialState: STATE,
  mapper: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefullMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>>;
export function statefullMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullMap.Name,
>(
  initialState: STATE,
  mapper: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefullMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>>;
export function statefullMap<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullMap.Name,
>(
  nameOrInitialState: NAME | STATE,
  initialStateOrMapper: STATE | statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
  mapper?: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<INPUT_STREAM, StatefullMap<INPUT_STREAM, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrInitialState === "string"
        ? new StatefullMap(nameOrInitialState, inputStream, initialStateOrMapper as STATE, mapper!)
        : new StatefullMap(
            NAME as NAME,
            inputStream,
            nameOrInitialState,
            initialStateOrMapper as statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
          ),
      inputStream,
    );
}

export namespace statefullMap {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) =>
    | [MAPPED | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]
    | Promise<[MAPPED | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip, Partial<STATE>]>;
}

import { Stream } from "../../streams/index.ts";

const NAME = "statefullMap";

export class StatefullMap<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SELF extends Stream<any, any> = never,
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
      [SELF] extends [never]
        ? StatefullMap<INPUT_STREAM, INPUT_NAME, SELF, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>
        : SELF
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;

  constructor(
    source: SOURCE,
    name = NAME as NAME,
    initialState: STATE,
    mapper: statefullMap.Mapper<
      CLEAN_VALUE,
      MAPPED,
      STATE,
      ERROR,
      StatefullMap<SOURCE, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>
    >,
  ) {
    super(name, async function* () {
      let state = initialState;
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;
        try {
          const maybePromise = mapper(state, cleanValue, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({
              type: "expected",
              value: cleanValue,
              detail: result.value,
              source: self,
            });

            yield Stream.sourceErr({
              value: cleanValue,
              detail: result.value,
              source: self,
            }) as never;

            continue;
          }
          const [mapped, newState] = result;

          state = { ...state, ...newState };

          yield mapped;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", source: self, value: cleanValue, detail: error.value });
            yield Stream.sourceErr({ value: value, detail: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", source: self, value: cleanValue, detail: error });
            yield Stream.sourceErr({ value: value, detail: error, source: self }) as never;
          }
        }
      }
    });
    const self = this;
  }

  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);

    return this._errors;
  }
}

export function statefullMap<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullMap.Name,
>(
  initialState: STATE,
  mapper: statefullMap.Mapper<
    CLEAN_VALUE,
    MAPPED,
    STATE,
    ERROR,
    StatefullMap<SOURCE, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>
  >,
): Stream.Transform<NAME, SOURCE, StatefullMap<SOURCE, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>> {
  return (_, source, name) => new StatefullMap(source, name, initialState, mapper);
}

export namespace statefullMap {
  export type Name = typeof NAME;
  export type Mapper<
    CLEAN_VALUE,
    MAPPED,
    STATE extends Record<string, unknown>,
    ERROR,
    SELF extends Stream<any, any>,
  > = (
    state: STATE,
    value: CLEAN_VALUE,
    self: SELF,
  ) => [MAPPED, Partial<STATE>] | Stream.Err<ERROR> | Promise<[MAPPED, Partial<STATE>] | Stream.Err<ERROR>>;
}

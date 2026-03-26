import { Stream } from "../../streams/index.ts";

const NAME = "statefullMap";

export class StatefullMap<
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
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
      Stream.Traversable<
        StatefullMap<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>,
        INPUT_NAME,
        INPUT_STREAM
      >
    >,
  NAME
> {
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR>, `${NAME}Errors`>;

  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      initialState: STATE;
      mapper: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>;
    },
  ) {
    const { initialState, inputStream, mapper, name } = options;
    super(name ?? (NAME as NAME), async function* () {
      let state = initialState;
      for await (const value of inputStream) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }

        try {
          const maybePromise = mapper(state, value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) {
            self._errors?.push({ type: "expected", value, error: result.value });

            yield Stream.sourceErr({ value, error: result.value, source: self }) as never;

            continue;
          }
          const [mapped, newState] = result;

          state = { ...state, ...newState };

          yield mapped;
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value, error: error.value });
            yield Stream.sourceErr({ value, error: error.value, source: self }) as never;
          } else {
            self._errors?.push({ type: "unexpected", value, error });
            yield Stream.sourceErr({ value, error, source: self }) as never;
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
  INPUT_STREAM extends Stream.AnyStream,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  STATE extends Record<string, unknown> = {},
  ERROR = never,
  NAME extends string = statefullMap.Name,
>(
  initialState: STATE,
  mapper: statefullMap.Mapper<CLEAN_VALUE, MAPPED, STATE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  NAME,
  StatefullMap<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, MAPPED, STATE, ERROR, NAME>
> {
  return (options) => new StatefullMap({ ...options, initialState, mapper });
}

export namespace statefullMap {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, STATE extends Record<string, unknown>, ERROR> = (
    state: STATE,
    value: CLEAN_VALUE,
  ) => [MAPPED, Partial<STATE>] | Stream.Err<ERROR> | Promise<[MAPPED, Partial<STATE>] | Stream.Err<ERROR>>;
}

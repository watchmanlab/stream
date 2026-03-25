import { Stream } from "../../streams/index.ts";

const NAME = "effect";

export class Effect<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = effect.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Effect<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
  NAME
> {
  protected _errors?: Stream<
    Stream.ErrorEvent<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Effect<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
    `${NAME}Errors`
  >;

  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      callback: effect.Callback<
        CLEAN_VALUE,
        ERROR,
        Stream.Traversable<Effect<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
      >;
    },
  ) {
    super(options.name ?? (NAME as NAME), async function* () {
      for await (const value of options.inputStream) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }

        const cleanValue = value as CLEAN_VALUE;

        try {
          const maybePromise = options.callback(cleanValue, self as never);

          if (maybePromise instanceof Promise) {
            maybePromise
              .then((error) => {
                if (error)
                  self._errors?.push({
                    type: "expected",
                    value: cleanValue,
                    error: error.value,
                    source: self as never,
                  });
              })
              .catch((error) => {
                if (Stream.isErr<ERROR>(error)) {
                  self._errors?.push({
                    type: "expected",
                    value: cleanValue,
                    error: error.value,
                    source: self as never,
                  });
                } else {
                  self._errors?.push({ type: "unexpected", value: cleanValue, error: error, source: self as never });
                }
              });
          }

          if (Stream.isErr(maybePromise)) {
            self._errors?.push({
              type: "expected",
              value: cleanValue,
              error: maybePromise.value,
              source: self as never,
            });
          }
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value: cleanValue, error: error.value, source: self as never });
          } else {
            self._errors?.push({ type: "unexpected", value: cleanValue, error, source: self as never });
          }
        } finally {
          yield value;
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
export function effect<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = effect.Name,
>(
  callback: effect.Callback<
    CLEAN_VALUE,
    ERROR,
    Stream.Traversable<Effect<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, INPUT_NAME, CLEAN_VALUE, ERROR, NAME>> {
  return (options) => new Effect({ ...options, callback });
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR, SELF extends Stream<any, any>> = (
    value: CLEAN_VALUE,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}

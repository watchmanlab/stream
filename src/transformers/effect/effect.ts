import { Stream } from "../../streams/index.ts";

const NAME = "effect";

export class Effect<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = effect.Name,
> extends Stream<
  | Stream.ExtractValue<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Effect<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  protected _expectedError?: Stream<{ value: CLEAN_VALUE; error: ERROR }, `${NAME}ExpectedError`>;
  protected _unexpectedError?: Stream<{ value: CLEAN_VALUE; error: unknown }, `${NAME}UnexpectedError`>;

  constructor(name: NAME, inputStream: INPUT_STREAM, callback: effect.Callback<CLEAN_VALUE, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        (async () => {
          if (Stream.isSentinel(value)) return;
          try {
            const result = await callback(value);
            if (Stream.isErr(result)) {
              self._expectedError?.push({ value, error: result.value });
            }
          } catch (error) {
            self._unexpectedError?.push({ value, error });
          }
        })();

        yield value;
      }
    });

    const self = this;
  }

  get expectedError() {
    if (!this._expectedError) this._expectedError = new Stream(`${this._name}ExpectedError` as never);
    return this._expectedError;
  }
  get unexpectedError() {
    if (!this._unexpectedError) this._unexpectedError = new Stream(`${this._name}UnexpectedError` as never);
    return this._unexpectedError;
  }
}

export function effect<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
  callback: effect.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Effect<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>>;

export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
>(
  callback: effect.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Effect<INPUT_STREAM, CLEAN_VALUE, ERROR, effect.Name>, INPUT_STREAM>
>;

export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = effect.Name,
>(
  nameOrCallback: NAME | effect.Callback<CLEAN_VALUE, ERROR>,
  callback?: effect.Callback<CLEAN_VALUE, ERROR>,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Effect<INPUT_STREAM, CLEAN_VALUE, ERROR, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrCallback === "string"
        ? new Effect(nameOrCallback, inputStream, callback!)
        : new Effect(NAME as NAME, inputStream, nameOrCallback),
      inputStream,
    );
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<CLEAN_VALUE, ERROR> = (
    value: CLEAN_VALUE,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;
}

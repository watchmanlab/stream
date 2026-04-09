import { Stream } from "../../streams/index.ts";

const NAME = "catchError";

export class CatchError<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = catchError.Name,
> extends Stream<
  | Stream.ExcludeSourceErr<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      SOURCE_ERR,
      ERROR,
      Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  protected _caught?: Stream<SOURCE_ERR, `${NAME}Caught`>;
  constructor(name: NAME, inputStream: INPUT_STREAM, callback?: catchError.Callback<CLEAN_VALUE, SOURCE_ERR, ERROR>) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (!Stream.isSourceErr(value)) {
          yield value;
          continue;
        }

        const sourceErr = value as SOURCE_ERR;

        self._caught?.push(sourceErr);

        if (!callback) continue;

        try {
          const maybePromise = callback(sourceErr);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (Stream.isErr(result)) yield Stream.sourceErr({ value: sourceErr, error: result.value, source: self });

          if (result) yield result;
        } catch (error) {
          yield Stream.sourceErr({ value: sourceErr, error, source: self });
        }
      }
    });
    const self = this;
  }

  get caught() {
    if (!this._caught) this._caught = new Stream(`${this._name}Caught` as never);
    return this._caught;
  }
}
export function catchError<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
>(): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, catchError.Name>, INPUT_STREAM>
>;
export function catchError<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, NAME>, INPUT_STREAM>
>;
export function catchError<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
>(
  name: NAME,
  callback: catchError.Callback<CLEAN_VALUE, SOURCE_ERR, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, NAME>, INPUT_STREAM>
>;
export function catchError<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
>(
  callback: catchError.Callback<CLEAN_VALUE, SOURCE_ERR, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, catchError.Name>, INPUT_STREAM>
>;
export function catchError<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SOURCE_ERR extends Stream.AnySourceErr = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = catchError.Name,
>(
  nameOrCallback?: NAME | catchError.Callback<CLEAN_VALUE, SOURCE_ERR, ERROR>,
  callback?: catchError.Callback<CLEAN_VALUE, SOURCE_ERR, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<CatchError<INPUT_STREAM, CLEAN_VALUE, SOURCE_ERR, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrCallback === "string"
        ? new CatchError(nameOrCallback, inputStream, callback)
        : new CatchError(NAME as NAME, inputStream, nameOrCallback),
      inputStream,
    );
}

export namespace catchError {
  export type Name = typeof NAME;
  export type Callback<CLEAN_VALUE, SOURCE_ERR, ERROR> = (
    error: [SOURCE_ERR] extends [never] ? Stream.SourceErr<unknown, unknown, Stream<unknown, string>> : SOURCE_ERR,
  ) =>
    | NoInfer<CLEAN_VALUE>
    | void
    | Stream.Err<ERROR>
    | Stream.Terminate
    | Stream.Skip
    | Promise<NoInfer<CLEAN_VALUE> | void | Stream.Err<ERROR> | Stream.Terminate | Stream.Skip>;
}

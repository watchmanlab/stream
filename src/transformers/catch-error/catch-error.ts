import { Stream } from "../../streams/index.ts";

const NAME = "catchError";

export class CatchError<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SOURCE_ERR extends Stream.SourceErr<any, any, any> = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = catchError.Name,
> extends Stream<
  | Stream.ExcludeSourceErr<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      SOURCE_ERR,
      ERROR,
      Stream.Transformer<CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
  NAME
> {
  protected _events?: Stream<
    catchError.Event<
      SOURCE_ERR,
      Stream.Transformer<CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
    `${NAME}Events`
  >;
  protected _errors?: Stream<
    Stream.ErrorEvent<
      SOURCE_ERR,
      ERROR,
      Stream.Transformer<CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
    >,
    `${NAME}Errors`
  >;
  constructor(
    options: Stream.TransformOptions<INPUT_STREAM, NAME> & {
      callback?: catchError.Callback<
        SOURCE_ERR,
        ERROR,
        Stream.Transformer<CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
      >;
    },
  ) {
    super(options.name ?? (NAME as NAME), async function* () {
      for await (const value of options.inputStream) {
        if (!Stream.isSourceErr(value)) {
          yield value;
          continue;
        }

        const sourceErr = value as SOURCE_ERR;

        self._events?.push({ type: "caught", sourceErr, self: self as never });

        if (!options.callback) continue;

        try {
          const maybePromise = options.callback(sourceErr, self as never);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (!result) continue;

          self._errors?.push({ type: "expected", value: sourceErr, error: result.value, source: self as never });

          yield Stream.sourceErr({ value: sourceErr, error: result.value, source: self });
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value: sourceErr, error: error.value, source: self as never });
            yield Stream.sourceErr({ value: sourceErr, error: error.value, source: self });
          } else {
            self._errors?.push({ type: "unexpected", value: sourceErr, error, source: self as never });
            yield Stream.sourceErr({ value: sourceErr, error, source: self });
          }
        }
      }
    });
    const self = this;
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}

export function catchError<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  SOURCE_ERR extends Stream.SourceErr<any, any, any> = Stream.ExtractSourceErr<INPUT_STREAM>,
  ERROR = never,
  NAME extends string = catchError.Name,
>(
  callback?: catchError.Callback<
    SOURCE_ERR,
    ERROR,
    Stream.Transformer<CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>, INPUT_NAME, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, NAME, CatchError<INPUT_STREAM, INPUT_NAME, SOURCE_ERR, ERROR, NAME>> {
  return (options) => new CatchError({ ...options, callback });
}

export namespace catchError {
  export type Name = typeof NAME;

  export type Callback<SOURCE_ERR, ERROR, SELF extends Stream<any, any>> = (
    error: [SOURCE_ERR] extends [never] ? Stream.SourceErr<unknown, unknown, Stream<unknown, string>> : SOURCE_ERR,
    self: SELF,
  ) => void | Stream.Err<ERROR> | Promise<void | Stream.Err<ERROR>>;

  export type Event<SOURCE_ERR, SELF extends Stream<any, any>> = {
    type: "caught";
    sourceErr: SOURCE_ERR;
    self: SELF;
  };
}

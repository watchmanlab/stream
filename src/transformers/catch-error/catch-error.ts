import { Stream } from "../../streams/index.ts";

const NAME = "catchError";

export class CatchError<
  SOURCE extends Stream<any, any>,
  SOURCE_ERR extends Stream.ExtractSourceErr<SOURCE> = Stream.ExtractSourceErr<SOURCE>,
  ERROR = never,
  NAME extends string = catchError.Name,
> extends Stream<
  | Stream.ExcludeSourceErr<SOURCE>
  | Stream.MaybeSourceErr<SOURCE_ERR, ERROR, CatchError<SOURCE, SOURCE_ERR, ERROR, NAME>>,
  NAME
> {
  protected _events?: Stream<catchError.Event<SOURCE_ERR, this>, `${NAME}Events`>;
  protected _errors?: Stream<Stream.ErrorEvent<SOURCE_ERR, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    callback?: catchError.Callback<SOURCE_ERR, ERROR, CatchError<SOURCE, SOURCE_ERR, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (!Stream.isSourceErr(value)) {
          yield value;
          continue;
        }

        const sourceErr = value as SOURCE_ERR;

        self._events?.push({ type: "caught", sourceErr, self });

        if (!callback) continue;

        try {
          const maybePromise = callback(sourceErr, self);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;

          if (!result) continue;

          self._errors?.push({ type: "expected", value: sourceErr, detail: result.value, source: self });

          yield Stream.sourceErr({ value: sourceErr, detail: result.value, source: self });
        } catch (error) {
          if (Stream.isErr<ERROR>(error)) {
            self._errors?.push({ type: "expected", value: sourceErr, detail: error.value, source: self });
            yield Stream.sourceErr({ value: sourceErr, detail: error.value, source: self });
          } else {
            self._errors?.push({ type: "unexpected", value: sourceErr, detail: error, source: self });
            yield Stream.sourceErr({ value: sourceErr, detail: error, source: self });
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
  SOURCE extends Stream<any, any>,
  SOURCE_ERR extends Stream.ExtractSourceErr<SOURCE> = Stream.ExtractSourceErr<SOURCE>,
  ERROR = never,
  NAME extends string = catchError.Name,
>(
  callback?: catchError.Callback<SOURCE_ERR, ERROR, CatchError<SOURCE, SOURCE_ERR, ERROR, NAME>>,
): Stream.Transformer<NAME, SOURCE, CatchError<SOURCE, SOURCE_ERR, ERROR, NAME>> {
  return (_, source, name) => new CatchError(source, name, callback);
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

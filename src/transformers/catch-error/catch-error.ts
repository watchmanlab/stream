import { Stream } from "../../streams";

const NAME = "catchError";

export class CatchError<VALUE, NAME extends string = catchError.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<catchError.Event<VALUE, NAME, ERROR>, `${NAME}-events`>;
  protected _errors?: Stream<catchError.ErrorEvent<VALUE, NAME, ERROR>, `${NAME}-errors`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback?: catchError.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();

      try {
        let next = await generator.next();
        while (!next.done) {
          const feedback = yield next.value;

          if (!Stream.Result.isSourceErr(feedback)) {
            next = await generator.next(feedback);
            continue;
          }

          self._events?.push({ type: "caught", error: feedback, self });

          if (!callback) {
            next = await generator.next();
            continue;
          }

          try {
            const result = await callback(feedback, self);

            if (!result) {
              next = await generator.next();
              continue;
            }

            self._errors?.push({ type: "expected", error: result.value, self });

            next = await generator.next(
              Stream.Result.sourceErr({ error: result.value, source: self, value: next.value }),
            );
          } catch (error) {
            if (Stream.Result.isErr(error)) {
              self._errors?.push({ type: "expected", error: error.value as ERROR, self });
            } else {
              self._errors?.push({ type: "unexpected", error: error, self });
            }
            next = await generator.next(Stream.Result.sourceErr({ error, source: self, value: next.value }));
          }
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}-errors` as never);
    return this._errors;
  }
}

export function catchError<VALUE, NAME extends string = catchError.Name, ERROR = unknown>(
  callback?: catchError.Callback<VALUE, NAME, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, CatchError<VALUE, NAME, ERROR>> {
  return (_, source, name) => new CatchError(source, name, callback);
}

export namespace catchError {
  export type Name = typeof NAME;

  export type Callback<VALUE, NAME extends string, ERROR> = (
    error: Stream.Result.SourceErr,
    self: CatchError<VALUE, NAME, ERROR>,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;

  export type Event<VALUE, NAME extends string, ERROR> = {
    type: "caught";
    error: Stream.Result.SourceErr;
    self: CatchError<VALUE, NAME, ERROR>;
  };

  export type ErrorEvent<VALUE, NAME extends string, ERROR> =
    | { type: "expected"; error: ERROR; self: CatchError<VALUE, NAME, ERROR> }
    | { type: "unexpected"; error: unknown; self: CatchError<VALUE, NAME, ERROR> };
}

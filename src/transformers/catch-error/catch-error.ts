import { Stream } from "../../streams";
import { consumer } from "../consumer";
import { each } from "../each";

const NAME = "catchError";

export class CatchError<
  VALUE,
  FALLBACK extends VALUE,
  NAME extends string = catchError.Name,
  ERROR = unknown,
> extends Stream<VALUE, NAME> {
  protected _events?: Stream<catchError.Event<VALUE, ERROR, this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback?: catchError.Callback<VALUE, FALLBACK, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      try {
        while (!result.done) {
          const error = yield result.value;
          console.log(error?.payload);

          if (!error) {
            result = await generator.next();
            continue;
          }

          self._events?.push({ type: "caught", error, self });

          if (!callback) {
            result = await generator.next();
            continue;
          }

          try {
            const value = await callback(error);

            if (!value) {
              result = await generator.next();
              continue;
            }

            if (value instanceof Stream.BoxError) {
              result = await generator.next(new Stream.SourceError(value.payload, self, result.value));
              continue;
            }

            self._events?.push({ type: "recovered", sourceValue: result.value, recoveredValue: value, self });

            const recoveryError = yield value;

            if (!recoveryError) {
              result = await generator.next();
              continue;
            }

            self._events?.push({
              type: "recovered-error",
              sourceValue: result.value,
              recoveredValue: value,
              self,
            });

            result = await generator.next(recoveryError);
          } catch (error) {
            if (error instanceof Stream.BoxError) {
              self._events?.push({ type: "expected-error", error: error.payload, self });
              result = await generator.next(new Stream.SourceError(error.payload, self, result.value));
            } else {
              self._events?.push({ type: "unexpected-error", error: error, self });
              result = await generator.next(new Stream.SourceError(error, self, result.value));
            }
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
}

export function catchError<VALUE, FALLBACK extends VALUE, NAME extends string = catchError.Name, ERROR = unknown>(
  callback?: catchError.Callback<VALUE, FALLBACK, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, CatchError<VALUE, FALLBACK, NAME, ERROR>> {
  return (_, source, name) => new CatchError(source, name, callback);
}

export namespace catchError {
  export type Name = typeof NAME;
  export type Callback<VALUE, FALLBACK extends VALUE, ERROR> = (
    sourceError: Stream.SourceError,
  ) => FALLBACK | void | Stream.BoxError<ERROR> | Promise<FALLBACK | void | Stream.BoxError<ERROR>>;

  export type Event<VALUE, ERROR, CATCH extends Stream<any, any>> =
    | { type: "expected-error"; error: ERROR; self: CATCH }
    | { type: "unexpected-error"; error: unknown; self: CATCH }
    | { type: "caught"; error: Stream.SourceError; self: CATCH }
    | { type: "recovered"; sourceValue: VALUE; recoveredValue: VALUE; self: CATCH }
    | { type: "recovered-error"; sourceValue: VALUE; recoveredValue: VALUE; self: CATCH };
}

new Stream([1, 2, 3])
  .pipe(
    catchError((error) => {
      //   console.log(error.value);

      return 4;
    }),
  )
  .pipe(
    each((v) => {
      if (v == 3) return new Stream.BoxError("error on  3");
      console.log(v);
    }),
  )
  .pipe(consumer());

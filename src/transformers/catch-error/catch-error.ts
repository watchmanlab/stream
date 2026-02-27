import { Stream } from "../../streams";
import { pump } from "../pump";
import { each } from "../each";

const NAME = "catchError";

export class CatchError<VALUE, NAME extends string = catchError.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<catchError.Event<VALUE, NAME, ERROR>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback?: catchError.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let next = await generator.next();

      try {
        while (!next.done) {
          const feedback = yield next.value;

          if (!Stream.Result.isSourceErr(feedback)) {
            next = await generator.next();
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

            // Transform error
            self._events?.push({ type: "expected-error", error: result.value, self });
            next = await generator.next(
              Stream.Result.sourceErr({ error: result.value, source: self, value: next.value }),
            );
          } catch (error) {
            if (Stream.Result.isErr(error)) {
              self._events?.push({ type: "expected-error", error: error.value as ERROR, self });
            } else {
              self._events?.push({ type: "unexpected-error", error: error, self });
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

  export type Event<VALUE, NAME extends string, ERROR> =
    | { type: "expected-error"; error: ERROR; self: CatchError<VALUE, NAME, ERROR> }
    | { type: "unexpected-error"; error: unknown; self: CatchError<VALUE, NAME, ERROR> }
    | { type: "caught"; error: Stream.Result.SourceErr; self: CatchError<VALUE, NAME, ERROR> };
}

new Stream([1, 2, 3])
  .pipe(
    catchError((error) => {
      // console.log(error.value);
      return Stream.Result.err("");
    }),
  )
  .pipe(
    each((v) => {
      if (v == 3) return Stream.Result.err("error on  3");

      console.log(v);
    }),
  )
  .pipe(pump());

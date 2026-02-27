import { Stream } from "../../streams";
import { pump } from "../pump";
import { each } from "../each";

const NAME = "catchDown";

export class CatchDown<VALUE, NAME extends string = catchDown.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<catchDown.Event<VALUE, NAME, ERROR>, `${NAME}-events`>;
  protected _errors?: Stream<catchDown.ErrorEvent<VALUE, NAME, ERROR>, `${NAME}-errors`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback?: catchDown.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();

      try {
        let next = await generator.next();
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
            self._errors?.push({ type: "expected-error", error: result.value, self });
            next = await generator.next(
              Stream.Result.sourceErr({ error: result.value, source: self, value: next.value }),
            );
          } catch (error) {
            if (Stream.Result.isErr(error)) {
              self._errors?.push({ type: "expected-error", error: error.value as ERROR, self });
            } else {
              self._errors?.push({ type: "unexpected-error", error: error, self });
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

export function catchDown<VALUE, NAME extends string = catchDown.Name, ERROR = unknown>(
  callback?: catchDown.Callback<VALUE, NAME, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, CatchDown<VALUE, NAME, ERROR>> {
  return (_, source, name) => new CatchDown(source, name, callback);
}

export namespace catchDown {
  export type Name = typeof NAME;

  export type Callback<VALUE, NAME extends string, ERROR> = (
    error: Stream.Result.SourceErr,
    self: CatchDown<VALUE, NAME, ERROR>,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;

  export type Event<VALUE, NAME extends string, ERROR> = {
    type: "caught";
    error: Stream.Result.SourceErr;
    self: CatchDown<VALUE, NAME, ERROR>;
  };

  export type ErrorEvent<VALUE, NAME extends string, ERROR> =
    | { type: "expected-error"; error: ERROR; self: CatchDown<VALUE, NAME, ERROR> }
    | { type: "unexpected-error"; error: unknown; self: CatchDown<VALUE, NAME, ERROR> };
}

new Stream([1, 2, 3])
  .pipe(
    catchDown((error) => {
      // console.log(error.value);
      return Stream.Result.err("kechmahaja" as const);
    }),
  )
  .pipe(
    each((v) => {
      if (v == 3) return Stream.Result.err("error on  3");

      console.log(v);
    }),
  )
  .pipe(pump())
  .each.catchDown.errors.pipe(each((v) => console.log(v)))
  .pipe(pump());

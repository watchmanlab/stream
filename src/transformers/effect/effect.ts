import { Stream } from "../../streams";
import { consumer } from "../consumer";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<effect.Event<this, ERROR>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      while (!result.done) {
        try {
          const error = await callback(result.value);

          if (error) {
            self._events?.push({ type: "expected-error", payload: error.payload, self });
            result = await generator.next(new Stream.Error(error.payload, self, result.value));
          } else {
            const error = yield result.value;
            result = await generator.next(error);
          }
        } catch (error: any) {
          if (error instanceof effect.Error) {
            self._events?.push({ type: "expected-error", payload: error.payload, self });
          } else {
            self._events?.push({ type: "unexpected-error", catched: error, self });
          }
          result = await generator.next(new Stream.Error(error, self, result.value));
        }
      }
    });

    const self = this;
  }

  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
}
export function effect<VALUE, NAME extends string = effect.Name, ERROR = unknown>(
  callback: effect.Callback<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Effect<VALUE, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}
export namespace effect {
  export type Name = typeof NAME;
  export class Error<ERROR> extends globalThis.Error {
    constructor(public readonly payload: ERROR) {
      super(typeof payload === "string" ? payload : undefined);
    }
  }
  export type Callback<VALUE, ERROR> = (value: VALUE) => Error<ERROR> | void | Promise<Error<ERROR> | void>;
  export type Event<SOURCE extends Stream<any, any>, ERROR> =
    | { type: "expected-error"; payload: ERROR; self: SOURCE }
    | { type: "unexpected-error"; catched: unknown; self: SOURCE };
}

new Stream([1, 2, 3])
  .pipe(
    effect((v) => {
      // return new Stream.Error("hello", v, "effect");
    }),
  )
  .pipe(
    consumer((v) => {
      // return new Stream.Error("hello", v, "effect");
    }),
  )
  .pipe(consumer((v) => console.log(v)));

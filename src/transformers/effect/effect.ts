import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<effect.Event<this, ERROR>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();

      let result = await generator.next();

      try {
        while (!result.done) {
          (async () => {
            try {
              const error = await callback(result.value);

              if (error) {
                self._events?.push({ type: "expected-error", error: error.payload, self });
              }
            } catch (error: any) {
              if (error instanceof Stream.BoxError) {
                self._events?.push({ type: "expected-error", error: error.payload, self });
              } else {
                self._events?.push({ type: "unexpected-error", error: error, self });
              }
            }
          })();

          const error = yield result.value;
          result = await generator.next(error);
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
export function effect<VALUE, NAME extends string = effect.Name, ERROR = unknown>(
  callback: effect.Callback<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Effect<VALUE, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => Stream.MaybeBoxError<ERROR> | Promise<Stream.MaybeBoxError<ERROR>>;
  export type Event<EFFECT extends Stream<any, any>, ERROR> =
    | { type: "expected-error"; error: ERROR; self: EFFECT }
    | { type: "unexpected-error"; error: unknown; self: EFFECT };
}

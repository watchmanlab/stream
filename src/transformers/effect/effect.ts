import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<effect.Event<ERROR, this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();

      let next = await generator.next();

      try {
        while (!next.done) {
          (async () => {
            try {
              const result = await callback(next.value);

              if (Stream.Result.isErr(result)) {
                self._events?.push({ type: "expected-error", error: result.error, self });
              }
            } catch (error: any) {
              if (error instanceof Stream.Result.Err) {
                self._events?.push({ type: "expected-error", error: error.error, self });
              } else {
                self._events?.push({ type: "unexpected-error", error, self });
              }
            }
          })();

          const error = yield next.value;
          next = await generator.next(error);
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
  ) => Stream.Result<void, ERROR> | Promise<Stream.Result<void, ERROR>>;
  export type Event<ERROR, EFFECT extends Effect<any, any, ERROR>> =
    | { type: "expected-error"; error: ERROR; self: EFFECT }
    | { type: "unexpected-error"; error: unknown; self: EFFECT };
}

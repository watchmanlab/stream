import { Stream } from "../../streams";

const NAME = "each";

export class Each<VALUE, NAME extends string = each.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<each.Event<this, ERROR>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: each.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      try {
        while (!result.done) {
          try {
            const callbackResult = await callback(result.value);

            if (!callbackResult.ok) {
              self._events?.push({ type: "expected-error", error: callbackResult.error, self });
              result = await generator.next(
                Stream.Yielded.err({ error: callbackResult.error, source: self, value: result.value }),
              );
            } else {
              const error = yield result.value;
              result = await generator.next(error);
            }
          } catch (error: any) {
            if (Stream.Result.isErr(error)) {
              self._events?.push({ type: "expected-error", error: error.error as ERROR, self });
            } else {
              self._events?.push({ type: "unexpected-error", error: error, self });
            }
            result = await generator.next(
              Stream.Result.err(Stream.error({ payload: error, source: self, value: result.value })),
            );
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
export function each<VALUE, NAME extends string = each.Name, ERROR = unknown>(
  callback: each.Callback<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, NAME>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => Stream.Result<VALUE, ERROR> | Promise<Stream.Result<VALUE, ERROR>>;
  export type Event<EACH extends Stream<any, any>, ERROR> =
    | { type: "expected-error"; error: ERROR; self: EACH }
    | { type: "unexpected-error"; error: unknown; self: EACH };
}

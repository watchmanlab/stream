import { Stream } from "../../streams";

const NAME = "each";

export class Each<VALUE, NAME extends string = each.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<each.Event<VALUE, NAME, ERROR>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: each.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let next = await generator.next();

      try {
        while (!next.done) {
          try {
            const result = await callback(next.value, self);

            if (Stream.Result.isErr(result)) {
              self._events?.push({ type: "expected-error", error: result.value, self });
              next = await generator.next(
                Stream.Result.sourceErr({ error: result.value, source: self, value: next.value }),
              );
            } else {
              const yielded = yield next.value;
              next = await generator.next(yielded);
            }
          } catch (error: any) {
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
export function each<VALUE, NAME extends string = each.Name, ERROR = unknown>(
  callback: each.Callback<VALUE, NAME, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Each<VALUE, NAME, ERROR>> {
  return (_, source, name) => new Each(source, name, callback);
}
export namespace each {
  export type Name = typeof NAME;

  export type Callback<VALUE, NAME extends string, ERROR> = (
    value: VALUE,
    self: Each<VALUE, NAME, ERROR>,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
  export type Event<VALUE, NAME extends string, ERROR> =
    | { type: "expected-error"; error: ERROR; self: Each<VALUE, NAME, ERROR> }
    | { type: "unexpected-error"; error: unknown; self: Each<VALUE, NAME, ERROR> };
}

import { Stream } from "../../streams";

const NAME = "retry";

export class Retry<VALUE, NAME extends string = retry.Name> extends Stream<VALUE, NAME> {
  protected _options: Required<retry.Options> = { maxAttempts: 3, delay: 0, backoff: "constant" };
  protected _events?: Stream<retry.Event<this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: retry.Options) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      try {
        while (!result.done) {
          let restAttempts = self._options.maxAttempts;

          let yielded: Stream.Yielded;

          while (restAttempts > 0) {
            yielded = yield result.value;

            if (yielded.ok) break;

            restAttempts--;

            self._events?.push({ type: "retry", attempts: self._options.maxAttempts - restAttempts, self });

            if (restAttempts > 0 && self._options.delay) {
              const delay =
                self._options.backoff === "exponential"
                  ? self._options.delay * Math.pow(2, self._options.maxAttempts - restAttempts - 1)
                  : self._options.delay;
              await new Promise((r) => setTimeout(r, delay));
            }
          }

          if (restAttempts > 0) {
            result = await generator.next(yielded!);
            continue;
          }
          self._events?.push({ type: "max-attempts-reached", attempts: self._options.maxAttempts, self });

          result = await generator.next(yielded!);
        }
      } finally {
        await generator.return();
      }
    });

    const self = this;
    this.options = options ?? {};
  }

  get options() {
    return this._options;
  }
  set options(options: retry.Options) {
    this._options = { ...this._options, ...options };
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
}

export function retry<VALUE, NAME extends string = retry.Name>(
  options?: retry.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, Retry<VALUE, NAME>> {
  return (_, source, name) => new Retry(source, name, options);
}

export namespace retry {
  export type Name = typeof NAME;

  export type Options = {
    maxAttempts?: number;
    delay?: number;
    backoff?: "constant" | "exponential";
  };

  export type Event<RETRY extends Retry<any, any>> =
    | { type: "retry"; attempts: number; self: RETRY }
    | { type: "max-attempts-reached"; attempts: number; self: RETRY };
}

import { Stream } from "../../streams";

const NAME = "retry";

export class Retry<VALUE, NAME extends string = retry.Name> extends Stream<VALUE, NAME> {
  protected _options: Required<retry.Options> = { maxAttempts: 3, delay: 0, backoff: "constant" };
  protected _events?: Stream<retry.Event<this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: retry.Options) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let next = await generator.next();

      try {
        while (!next.done) {
          let restAttempts = self._options.maxAttempts;

          let feedback: unknown;

          while (restAttempts > 0) {
            feedback = yield next.value;

            if (!Stream.Result.isSourceErr(feedback)) break;

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
            next = await generator.next(feedback);
            continue;
          }
          self._events?.push({ type: "max-attempts-reached", attempts: self._options.maxAttempts, self });

          next = await generator.next(feedback);
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
): Stream.Transform<NAME, Stream<VALUE, any>, Retry<VALUE, NAME>> {
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

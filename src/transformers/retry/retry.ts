import { Stream } from "../../streams";

const NAME = "retry";

export class Retry<VALUE, NAME extends string = retry.Name> extends Stream<VALUE, NAME> {
  protected _options: Required<retry.Options> = { maxAttempts: 3, delay: 0, backoff: "constant" };
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: retry.Options) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      while (!result.done) {
        let restAttempts = self._options.maxAttempts;

        let maybeError: Stream.MaybeError;

        while (restAttempts > 0) {
          maybeError = yield result.value;

          if (!maybeError) break;

          restAttempts--;

          if (restAttempts > 0 && self._options.delay) {
            const delay =
              self._options.backoff === "exponential"
                ? self._options.delay * Math.pow(2, self._options.maxAttempts - restAttempts - 1)
                : self._options.delay;
            await new Promise((r) => setTimeout(r, delay));
          }
        }

        if (restAttempts > 0) {
          result = await generator.next();
        } else {
          result = await generator.next(new Stream.Error("max attempts reached", self, result.value));
        }
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
}

export function retry<VALUE, NAME extends string = retry.Name>(
  options?: retry.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, Retry<VALUE, NAME>> {
  return (_, source, name) => new Retry(source, name, options);
}

export namespace retry {
  export type Name = typeof NAME;

  export type ErrorMessage = "max-attempts-reached";
  export type Options = {
    maxAttempts?: number;
    delay?: number;
    backoff?: "constant" | "exponential";
  };
}

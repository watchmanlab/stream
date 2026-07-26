import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "$resolve",
> extends Transformer<INPUT, VALUE, NAME> {
  declare protected _options: Resolve.Options<VALUE, NAME>;
  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<VALUE, NAME>) {
    let count = 0;
    const inputConsumer = input.consume((self, maybePromise) => {
      if (++count < concurrency) self.next();

      if (maybePromise instanceof Promise) {
        maybePromise
          .then((value) => this.push(value))
          .catch((error) => {
            this._options.error?.(this, error);
            this._options.$error?.push(error);
            self.next();
          })
          .finally(() => {
            count--;
          });
      } else {
        this.push(maybePromise);
      }
    });
    super(input, {
      ...options,
      name: options?.name ?? ("$resolve" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        options?.next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }

  get $error() {
    this._options.$error ??= new Stream({ scope: this });
    return new Stream({ name: `${this.name}Error`, source: this._options.$error });
  }
}

export function resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  NAME extends NonEmptyString = "$resolve",
>(concurrency = 1, options?: Resolve.Options<VALUE, NAME>): Transform<INPUT, Resolve<INPUT, VALUE, NAME>> {
  return (input) => new Resolve(input, concurrency, options);
}

export namespace Resolve {
  export type Options<VALUE, NAME extends NonEmptyString> = Stream.Options<VALUE, NAME> & {
    error?: (self: Stream<VALUE, NAME>, error: unknown) => void;
    $error?: Stream<unknown, any>;
  };
}

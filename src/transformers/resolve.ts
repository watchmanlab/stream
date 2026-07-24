import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, Transform } from "../core/types";

export class Resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
> extends Transformer<INPUT, VALUE> {
  declare protected _options: Resolve.Options<VALUE>;
  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<VALUE>) {
    let count = 0;
    const inputConsumer = input.listen((self, maybePromise) => {
      if (++count < concurrency) self.next();

      if (maybePromise instanceof Promise) {
        maybePromise
          .then((value) => this.push(value))
          .catch((error) => {
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
      next(self, consumer) {
        inputConsumer.next();
        options?.next?.(self, consumer);
      },
      terminate(self, reason) {
        options?.terminate?.(self, reason);
      },
    });
  }

  get $error() {
    return (this._options.$error ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$error = undefined;
      },
    }));
  }
}

export function resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
>(concurrency = 1, options?: Resolve.Options<VALUE>): Transform<INPUT, Resolve<INPUT, VALUE>> {
  return (input) => new Resolve(input, concurrency, options);
}

export namespace Resolve {
  export type Options<VALUE> = Stream.Options<VALUE> & { error?: (error: unknown) => void; $error?: Stream<unknown> };
}

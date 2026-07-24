import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, Transform } from "../core/types";

export class Resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
> extends Transformer<INPUT, VALUE> {
  private _$error?: Stream<unknown>;
  constructor(input: INPUT, concurrency = 1, options?: Stream.Options<VALUE>) {
    let count = 0;
    const inputConsumer = input.listen((self, maybePromise) => {
      if (++count < concurrency) self.next();

      if (maybePromise instanceof Promise) {
        maybePromise
          .then((value) => this.push(value))
          .catch((error) => {
            this._$error?.push(error);
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
    return (this._$error ??= new Stream({
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$error = undefined;
      },
    }));
  }
}

export function resolve<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
>(concurrency = 1, options?: Stream.Options<VALUE>): Transform<INPUT, Resolve<INPUT, VALUE>> {
  return (input) => new Resolve(input, concurrency, options);
}

import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, TerminateReason, Transform } from "../core/types";

export class Resolve<
  INPUT extends Stream<Promise<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$resolve",
> extends Transformer<INPUT, VALUE, NAME> {
  declare protected _options: Resolve.Options<VALUE, NAME>;
  declare protected _metaStreams: Resolve.MetaStreams<VALUE>;

  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<VALUE, NAME>) {
    options = { ...options };

    let count = 0;
    const inputConsumer = input.consume((self, maybePromise) => {
      if (++count < concurrency) self.next();

      maybePromise
        .then((value) => {
          count--;

          this.push(value);
        })
        .catch((error) => {
          count--;
          this._options.error?.(this, error);
          this._metaStreams.$error?.push(error);
          self.next();
        });
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$resolve" as NAME),
      next: (self, consumer) => {
        options.next?.(self, consumer);
        if (count < concurrency) inputConsumer.next();
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        options.terminate?.(self, reason);
      },
    });
  }

  get $error() {
    this._metaStreams.$error ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Error`, source: this._metaStreams.$error });
  }
}

export function resolve<
  INPUT extends Stream<Promise<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$resolve",
>(concurrency = 1, options?: Resolve.Options<VALUE, NAME>): Transform<INPUT, Resolve<INPUT, VALUE, NAME>> {
  return (input) => new Resolve(input, concurrency, options);
}

export namespace Resolve {
  export type Options<VALUE, NAME extends NonEmptyString> = Stream.Options<VALUE, NAME> & {
    error?: (self: Stream<VALUE, NAME>, error: unknown) => void;
  };
  export type MetaStreams<VALUE> = Stream.MetaStreams<VALUE> & { $error?: Stream<unknown, any> };
}

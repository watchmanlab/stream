import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { ExtractValue, NonEmptyString, TerminateReason, Transform } from "../core/types";
import { Signal } from "../streams/signal";

export class Resolve<
  INPUT extends Stream<Promise<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$resolve",
> extends Transformer<INPUT, VALUE, NAME> {
  declare protected _options: Resolve.Options<VALUE, NAME>;
  declare protected _metaStreams: Resolve.MetaStreams<VALUE, NAME>;

  constructor(input: INPUT, concurrency = 1, options?: Resolve.Options<VALUE, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const $terminate = new Signal<TerminateReason>();

    let count = 0;

    const inputConsumer = input.consume(
      (self, maybePromise) => {
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
          })
          .finally(() => {
            if (!count && (self.status === "abort" || self.status === "complete")) {
              $terminate.push(self.status);
            }
          });
      },
      {
        terminate(_, reason) {
          if (count) return;
          $terminate.push(reason);
        },
      },
    );

    super(input, {
      ...rest,
      name: name ?? ("$resolve" as NAME),
      $terminate,
      next: (self, consumer) => {
        if (count < concurrency) inputConsumer.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        $terminate.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }

  get $error() {
    this._metaStreams.$error ??= new Stream({ name: `${this.name}MetaError`, $terminate: this.$terminate });
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
  export type MetaStreams<VALUE, NAME extends NonEmptyString> = Stream.MetaStreams<VALUE, NAME> & {
    $error?: Stream<unknown, `${NAME}MetaError`>;
  };
}

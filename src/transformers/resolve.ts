import { Stream } from "../core/stream";
import type { ExtractValue, NonEmptyString, TerminateReason, Transform } from "../core/types";
import { Signal } from "../streams/signal";

export function resolve<
  INPUT extends Stream<Promise<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$resolve",
>(
  concurrency = 1,
  options?: Resolve.Options<VALUE, NAME>,
): Transform<INPUT, NAME, Stream<VALUE, NAME> & { $error: Stream<unknown, `${NAME}Error`> }> {
  return (input) => {
    const { name, next, terminate, error, ...rest } = options ?? {};

    const $terminate = new Signal<TerminateReason>();

    let $error: Stream<unknown> | undefined;

    const output = new Stream({
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
        $error?.terminate(reason);
        $error = undefined;
        terminate?.(self, reason);
      },
    });

    let count = 0;

    const inputConsumer = input.consume(
      (self, maybePromise) => {
        if (++count < concurrency) self.next();

        maybePromise
          .then((value) => {
            count--;
            output.push(value);
          })
          .catch((error) => {
            count--;
            error?.(output, error);
            $error?.push(error);
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

    return Object.defineProperty(output, "$error", {
      get() {
        $error ??= new Stream({ $terminate: output.$terminate });
        return new Stream({ name: `${output.name}Error`, source: $error, $terminate: output.$terminate });
      },
    }) as Stream<VALUE, NAME> & { $error: Stream<unknown, `${NAME}Error`> };
  };
}

export namespace Resolve {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source"> & {
    error?: (self: Stream<VALUE, NAME>, error: unknown) => void;
  };
}

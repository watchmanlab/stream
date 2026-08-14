import { Producer } from "../core/producer";
import type { ExtractValue, NonEmptyString, TerminateReason, Transform } from "../core/types";
import { Signal } from "../streams/signal";

export function resolve<
  INPUT extends Producer<Promise<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$resolve",
>(
  concurrency = 1,
  options?: Resolve.Options<VALUE, NAME>,
): Transform<INPUT, NAME, Producer<VALUE, NAME> & { $error: Producer<unknown, `${NAME}Error`> }> {
  return (input) => {
    const { name, next, terminate, error, ...rest } = options ?? {};

    const $terminate = new Signal<TerminateReason>();

    let $error: Producer<unknown> | undefined;

    const output = new Producer({
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
        $error ??= new Producer({ $terminate: output.$terminate });
        return new Producer({ name: `${output.name}Error`, source: $error, $terminate: output.$terminate });
      },
    }) as Producer<VALUE, NAME> & { $error: Producer<unknown, `${NAME}Error`> };
  };
}

export namespace Resolve {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Producer.Options<VALUE, NAME>, "source"> & {
    error?: (self: Producer<VALUE, NAME>, error: unknown) => void;
  };
}

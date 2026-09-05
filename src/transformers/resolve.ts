import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable, ValueOfPromise, Error } from "../core/types";

/**
 * Resolves a stream of `Promise`s with optional concurrency control.
 * Emits resolved values or `Error<reason>` for rejections.
 * Upstream credit is withheld until a concurrency slot is free.
 *
 * @example
 * fromGenerator(function* () {
        yield fetch("/a");
        yield fetch("/b");
        yield fetch("/c");
      }).pipe(resolve(2)).pipe(listen(console.log));
 */
export class Resolve<
  INPUT extends Consumable<Promise<any>>,
  VALUE extends ValueOfPromise<ValueOfConsumable<INPUT>> = ValueOfPromise<ValueOfConsumable<INPUT>>,
> extends Source<VALUE | Error<any>> {
  constructor(
    readonly $input: INPUT,
    private concurrency = 1,
  ) {
    super();
  }
  consume(
    handler: Consumer.Handler<VALUE | Error<any>>,
    options?: Consumer.Options<VALUE | Error<any>>,
  ): Consumer<VALUE | Error<any>> {
    const { next, terminate, ...rest } = options ?? {};

    let count = 0;

    const output$ = new Consumer(handler, {
      ...rest,
      next: (c) => {
        if (count < this.concurrency) input$.next();
        next?.(c);
      },
      terminate(c, r) {
        input$.terminate(r);
        terminate?.(c, r);
      },
    });

    const input$ = this.$input.consume(
      (c, maybePromise) => {
        maybePromise
          .then((v) => {
            count--;
            output$.push(v);
          })
          .catch((error) => {
            count--;
            output$.push(new Error(error));
          })
          .finally(() => {
            if (!count && (c.status === "abort" || c.status === "complete")) {
              output$.terminate(c.status);
            }
          });

        if (++count < this.concurrency) c.next();
      },
      {
        terminate(_, reason) {
          if (count) return;
          output$.terminate(reason);
        },
      },
    );
    return output$;
  }
}
/**
 * Resolves a stream of `Promise`s with optional concurrency control.
 * Emits resolved values or `Error<reason>` for rejections.
 * Upstream credit is withheld until a concurrency slot is free.
 *
 * @param concurrency Max number of in-flight promises (default `1`).
 *
 * @example
 * fromGenerator(function* () {
        yield fetch("/a");
        yield fetch("/b");
        yield fetch("/c");
      }).pipe(resolve(2)).pipe(listen(console.log));
 */
export function resolve<INPUT extends Consumable<Promise<any>>>(concurrency = 1) {
  return ($input: INPUT) => new Resolve($input, concurrency);
}

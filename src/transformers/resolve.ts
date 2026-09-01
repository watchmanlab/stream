import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable, ValueOfPromise, Error } from "../core/types";

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
      next: (consumer) => {
        if (count < this.concurrency) input$.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        input$.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const input$ = this.$input.consume(
      (consumer, maybePromise) => {
        if (++count < this.concurrency) consumer.next();

        maybePromise
          .then((value) => {
            count--;
            output$.push(value);
          })
          .catch((error) => {
            count--;
            output$.push(new Error(error));
          })
          .finally(() => {
            if (!count && (consumer.status === "abort" || consumer.status === "complete")) {
              output$.terminate(consumer.status);
            }
          });
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

export function resolve<INPUT extends Consumable<Promise<any>>>(concurrency = 1) {
  return ($input: INPUT) => new Resolve($input, concurrency);
}

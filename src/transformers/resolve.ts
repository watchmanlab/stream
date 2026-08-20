import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import type { ExtractValue, Result } from "../core/types";

export class Resolve<
  INPUT extends Consumable<Promise<any>>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
>
  extends Source<Result<VALUE, any>>
  implements Transformer<INPUT, Result<VALUE, any>>
{
  constructor(
    readonly $input: INPUT,
    private concurrency = 1,
  ) {
    super();
  }
  consume(
    handler: Consumer.Handler<Result<VALUE, any>>,
    options?: Consumer.Options<Result<VALUE, any>>,
  ): Consumer<Result<VALUE, any>> {
    const { next, terminate, ...rest } = options ?? {};

    let count = 0;

    const inputConsumer = this.$input.consume(
      (consumer, maybePromise) => {
        if (++count < this.concurrency) consumer.next();

        maybePromise
          .then((value) => {
            count--;
            outputConsumer.push({ ok: true, value });
          })
          .catch((error) => {
            count--;
            outputConsumer.push({ ok: false, error });
          })
          .finally(() => {
            if (!count && (consumer.status === "abort" || consumer.status === "complete")) {
              outputConsumer.terminate(consumer.status);
            }
          });
      },
      {
        terminate(_, reason) {
          if (count) return;
          outputConsumer.terminate(reason);
        },
      },
    );

    const outputConsumer = new Consumer(handler, {
      ...rest,
      next: (consumer) => {
        if (count < this.concurrency) inputConsumer.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        inputConsumer.terminate(reason);
        terminate?.(consumer, reason);
      },
    });
    return outputConsumer;
  }
}

export function resolve<INPUT extends Consumable<Promise<any>>>(concurrency = 1) {
  return ($input: INPUT) => new Resolve($input, concurrency);
}

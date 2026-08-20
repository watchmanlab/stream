import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class KeepNewest<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private maxSize: number,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};
    const { $input, maxSize } = this;

    const input$ = $input
      .consume(
        (consumer, value) => {
          if (output$.credit > 0) output$.push(value);
          consumer.next();
        },
        {
          queueFactory: () => new DefaultSizedQueue(maxSize, { dropStrategy: "oldest" }),
          terminate(_, reason) {
            output$.terminate(reason);
          },
        },
      )
      .next();

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        next?.(consumer);
      },
      terminate(consumer, reason) {
        terminate?.(consumer, reason);
        input$.terminate(reason);
      },
    });

    return output$;
  }
}

export function keepNewest<INPUT extends Consumable.AnyConsumable>(maxSize: number) {
  return ($input: INPUT) => new KeepNewest($input, maxSize);
}

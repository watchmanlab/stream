import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class KeepNewest<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
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

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        next?.(consumer);
        input$.next();
      },
      terminate(consumer, reason) {
        terminate?.(consumer, reason);
        input$.terminate(reason);
      },
    });

    const input$ = $input.consume(
      (consumer, value) => {
        output$.push(value);
        consumer.next();
      },
      {
        queueFactory: () => new DefaultSizedQueue(maxSize, { dropStrategy: "oldest" }),
      },
    );

    return output$;
  }
}

export function keepNewest<INPUT extends AnyConsumable>(maxSize: number) {
  return ($input: INPUT) => new KeepNewest($input, maxSize);
}

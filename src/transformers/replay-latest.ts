import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class ReplayLatest<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  private queue?: DefaultSizedQueue<VALUE>;
  constructor(
    private $input: INPUT,
    last: number,
  ) {
    super();

    $input
      .consume(
        (c, v) => {
          (this.queue ??= new DefaultSizedQueue(last)).enqueue(v);
          c.next();
        },
        {
          terminate: () => {
            this.queue?.clear();
            this.queue = undefined;
          },
        },
      )
      .next();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const input$ = this.$input.consume((c, v) => output$.push(v), {
      terminate(consumer, reason) {
        output$.terminate(reason);
      },
    });

    const output$ = new Consumer(handler, {
      ...rest,
      next(consumer) {
        input$.next();
        next?.(consumer);
      },
      terminate(consumer, reason) {
        input$.terminate(reason);
        terminate?.(consumer, reason);
      },
    }).pushBatch([...(this.queue ?? [])]);

    return output$;
  }
}

export function replayLatest<INPUT extends Consumable.AnyConsumable>(last: number) {
  return ($input: INPUT) => new ReplayLatest($input, last);
}

import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Latests<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  private queue?: DefaultSizedQueue<VALUE>;
  constructor(
    readonly $input: INPUT,
    count: number,
  ) {
    super();

    $input
      .consume((c, v) => {
        (this.queue ??= new DefaultSizedQueue(count)).enqueue(v);
        c.next();
      })
      .next();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

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

    this.queue?.clear();
    this.queue = undefined;

    const input$ = this.$input.consume((c, v) => output$.push(v), {
      terminate(consumer, reason) {
        output$.terminate(reason);
      },
    });
    return output$;
  }
}

export function latests<INPUT extends Consumable.AnyConsumable>(count: number) {
  return ($input: INPUT) => new Latests($input, count);
}

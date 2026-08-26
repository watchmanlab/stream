import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class BufferLatest<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  private queue?: DefaultSizedQueue<VALUE>;
  constructor(
    readonly $input: INPUT,
    maxSize: number,
  ) {
    super();

    $input
      .consume((c, v) => {
        (this.queue ??= new DefaultSizedQueue(maxSize)).enqueue(v);
        c.next();
      })
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

    this.queue?.clear();
    this.queue = undefined;
    return output$;
  }
}

export function bufferLatest<INPUT extends Consumable.AnyConsumable>(maxSize: number) {
  return ($input: INPUT) => new BufferLatest($input, maxSize);
}

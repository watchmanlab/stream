import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { Stream } from "../core/stream";

import { ExtractValue } from "../core/types";

export class ReplayLatest<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  private queue?: DefaultSizedQueue<VALUE>;

  constructor(
    private $input: INPUT,
    last: number,
  ) {
    super();

    this.$input
      .consume((consumer, value) => ((this.queue ??= new DefaultSizedQueue(last)).enqueue(value), consumer.next()), {
        terminate: () => (this.queue?.clear(), (this.queue = undefined)),
      })
      .next();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return this.$input.consume(handler, options).pushBatch([...(this.queue ?? [])]);
  }
}

export function replayLatest<INPUT extends Consumable.AnyConsumable>(last: number) {
  return ($input: INPUT) => new ReplayLatest($input, last);
}

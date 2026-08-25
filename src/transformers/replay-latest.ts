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
  private _queue: DefaultSizedQueue<VALUE>;
  constructor(
    private $input: INPUT,
    private last: number,
  ) {
    super();

    this._queue = new DefaultSizedQueue(last);
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const input$ = this.$input.consume((_, value) => {
      // this.push(value);
      this._queue.enqueue(value);
    });

    return input$;
  }
}

export function replayLatest<INPUT extends Consumable.AnyConsumable>(last: number) {
  return ($input: INPUT) => new ReplayLatest($input, last);
}

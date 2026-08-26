import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { ExtractValue } from "../core/types";

export class BufferLatest<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
> extends Source<VALUE> {
  constructor(
    readonly $input: INPUT,
    private maxSize: number,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { queueFactory, ...rest } = options ?? {};
    const { maxSize } = this;
    return this.$input.consume(handler, {
      ...rest,
      queueFactory: () => new DefaultSizedQueue(maxSize),
    });
  }
}

export function bufferLatest<INPUT extends Consumable.AnyConsumable>(maxSize: number) {
  return ($input: INPUT) => new BufferLatest($input, maxSize);
}

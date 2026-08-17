import { Consumer } from "../core/consumer";
import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class DropOldest<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
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
    const { queueFactory, ...rest } = options ?? {};
    return this.$input.consume(handler, {
      ...rest,
      queueFactory: () => new DefaultSizedQueue(this.maxSize, { dropStrategy: "oldest" }),
    });
  }
}

export function dropOldest<INPUT extends AnyConsumable>(maxSize: number) {
  return ($input: INPUT) => new DropOldest($input, maxSize);
}

import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class HotDelay<
  INPUT extends AnyConsumable,
  MS extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private ms: MS,
  ) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};
    let timer = null as any;

    return this.$input.consume(
      (consumer, value) => {
        //
      },
      {
        ...rest,

        terminate(consumer, reason) {
          terminate?.(consumer, reason);
          clearTimeout(timer);
        },
      },
    );
  }
}

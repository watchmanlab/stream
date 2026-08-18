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
    const { init, ...rest } = options ?? {};
    let timer = null as any;
    let last: VALUE;

    return this.$input.consume(
      (consumer, value) => {
        last = value;
        //
        //
      },
      {
        ...rest,
        init: (consumer) => {
          const cleanup = init?.(consumer);
          timer = setTimeout(() => {
            consumer.push(last);
          }, this.ms);

          return (reason) => {
            cleanup?.(reason);
            clearTimeout(timer);
          };
        },
      },
    );
  }
}

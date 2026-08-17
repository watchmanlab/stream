import { EMPTY } from "../core/consts";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, Empty, ExtractValue, Transformer } from "../core/types";

export class Throttle<
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
    let last: VALUE | Empty = EMPTY;

    return this.$input
      .consume(
        (consumer, value) => {
          consumer.next();
          last = value;

          if (timer) return;

          timer = setTimeout(() => {
            if (last !== EMPTY) {
              handler(consumer, last);
              last = EMPTY;
            }
            timer = null;
          }, this.ms);
        },
        {
          ...rest,
          terminate(consumer, reason) {
            if (reason === "complete" && last !== EMPTY) {
              handler(consumer, last);
            }
            clearTimeout(timer);
            last = EMPTY;
            terminate?.(consumer, reason);
          },
        },
      )
      .next();
  }
}

export function throttle<INPUT extends AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Throttle($input, ms);
}

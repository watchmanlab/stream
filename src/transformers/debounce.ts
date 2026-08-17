import { EMPTY } from "../core/consts";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { AnyConsumable, Empty, ExtractValue, Transformer } from "../core/types";

export class Debounce<
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

    return new Consumer(handler, {
      ...rest,
      init: (outputconsumer) => {
        let timer = null as any;
        let last: VALUE | Empty;
        const inputConsmuer = this.$input
          .consume(
            (consumer, value) => {
              consumer.next();
              last = value;

              clearTimeout(timer);
              timer = setTimeout(() => {
                outputconsumer.push(value);
                last = EMPTY;
              }, this.ms);
            },
            {
              terminate(_, reason) {
                if (reason === "complete" && last !== EMPTY) {
                  outputconsumer.push(last);
                }
                clearTimeout(timer);
                outputconsumer.terminate(reason);
              },
            },
          )
          .next();
        const cleanup = init?.(outputconsumer);
        return (reason) => {
          cleanup?.(reason);
          inputConsmuer.terminate(reason);
        };
      },
    });
  }
}

export function debounce<INPUT extends AnyConsumable, MS extends number>(ms: MS) {
  return ($input: INPUT) => new Debounce($input, ms);
}

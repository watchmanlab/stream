import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Flat<
  INPUT extends Consumable<Array<any>>,
  DEPTH extends number = 0,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>
  extends Source<FlatArray<VALUE, DEPTH>>
  implements Transformer<INPUT, FlatArray<VALUE, DEPTH>>
{
  constructor(
    readonly $input: INPUT,
    private depth = 0 as DEPTH,
  ) {
    super();
  }

  consume(
    handler: Consumer.Handler<FlatArray<VALUE, DEPTH>>,
    options?: Consumer.Options<FlatArray<VALUE, DEPTH>>,
  ): Consumer<FlatArray<VALUE, DEPTH>> {
    const { next, terminate, ...rest } = options ?? {};

    let cursor = 0;
    let values = [] as any[];

    const inputConsumer = this.$input.consume((consumer, value) => {
      if (!value.length) {
        consumer.next();
        return;
      }

      values = this.depth === 0 ? value : value.flat(this.depth);
      cursor = 0;

      handler(outputConsumer, values[cursor++]);
    });

    const outputConsumer = new Consumer<FlatArray<VALUE, DEPTH>>(handler, {
      ...rest,
      next(consumer) {
        if (cursor === values.length) {
          inputConsumer.next();
        } else {
          handler(consumer, values[cursor++]);
        }
        next?.(consumer);
      },
      terminate(consumer, reason) {
        inputConsumer.terminate(reason);
        terminate?.(consumer, reason);
      },
    });
    return outputConsumer;
  }
}

export function flat<INPUT extends Consumable<Array<any>>, DEPTH extends number = 0>(depth = 0 as DEPTH) {
  return ($input: INPUT) => new Flat($input, depth);
}

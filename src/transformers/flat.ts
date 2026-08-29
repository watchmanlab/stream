import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Flat<
  INPUT extends Consumable<Array<any>>,
  DEPTH extends number = 0,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<FlatArray<VALUE, DEPTH>> {
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

    const output$ = new Consumer<FlatArray<VALUE, DEPTH>>(handler, {
      ...rest,
      next(consumer) {
        if (cursor === values.length) {
          input$.next();
        } else {
          handler(consumer, values[cursor++]);
        }
        next?.(consumer);
      },
      terminate(consumer, reason) {
        input$.terminate(reason);
        terminate?.(consumer, reason);
      },
    });

    const input$ = this.$input.consume((consumer, value) => {
      if (!value.length) {
        consumer.next();
        return;
      }

      values = this.depth === 0 ? value : value.flat(this.depth);
      cursor = 0;

      handler(output$, values[cursor++]);
    });
    return output$;
  }
}

export function flat<INPUT extends Consumable<Array<any>>, DEPTH extends number = 0>(depth = 0 as DEPTH) {
  return ($input: INPUT) => new Flat($input, depth);
}

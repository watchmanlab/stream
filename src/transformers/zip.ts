import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ZipedArray } from "../core/types";

export class Zip<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
> extends Source<ZipedArray<[INPUT, ...OTHERS]>> {
  private others: OTHERS;
  constructor(
    private $input: INPUT,
    ...others: OTHERS
  ) {
    super();
    this.others = others;
  }
  override consume(
    handler: Consumer.Handler<ZipedArray<[INPUT, ...OTHERS]>>,
    options?: Consumer.Options<ZipedArray<[INPUT, ...OTHERS]>>,
  ): Consumer<ZipedArray<[INPUT, ...OTHERS]>> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.others.length + 1).fill(EMPTY);

    const consumers$ = [];

    const output$ = new Consumer<ZipedArray<[INPUT, ...OTHERS]>>(handler, {
      ...rest,
      next(consumer) {
        if (buffer.every((v) => v !== EMPTY)) {
          consumer.push(buffer as any);
        }
        next?.(consumer);
      },
      terminate(consumer, reason) {
        terminate?.(consumer, reason);
      },
    });
    return output$;
  }
}

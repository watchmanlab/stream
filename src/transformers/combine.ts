import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { Empty, ValueOfConsumable, ZipedArray } from "../core/types";

export class Combine<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
  VALUE extends ZipedArray<[INPUT, ...OTHERS]> = ZipedArray<[INPUT, ...OTHERS]>,
> extends Source<VALUE> {
  private $inputs: [INPUT, ...OTHERS];

  constructor($input: INPUT, $others: OTHERS) {
    super();
    this.$inputs = [$input, ...$others];
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.$inputs.length).fill(EMPTY);
    let count = buffer.length;

    const consumers$ = this.$inputs.map(($input, index) =>
      $input.consume(
        (_, value) => {
          buffer[index] = value;

          if (!--count) {
            count = buffer.length;
            const values = [...buffer];
            buffer.fill(EMPTY);
            output$.push(values as any);
          }
        },
        {
          terminate(consumer, reason) {
            output$.terminate(reason);
          },
        },
      ),
    );

    const output$ = new Consumer<VALUE>(handler, {
      ...rest,
      next(consumer) {
        consumers$.forEach((c) => c.next());
        next?.(consumer);
      },
      terminate: (consumer, reason) => {
        consumers$.forEach((c) => c.terminate(reason));
        consumers$.length = 0;
        buffer.length = 0;
        terminate?.(consumer, reason);
      },
    });
    return output$;
  }
}

export function combine<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Combine($input, others);
}

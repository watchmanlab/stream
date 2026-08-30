import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

export class Batch<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE[]> {
  constructor(
    readonly $input: INPUT,
    private size: number,
  ) {
    super();
  }

  consume(handler: Consumer.Handler<VALUE[]>, options?: Consumer.Options<VALUE[]>): Consumer<VALUE[]> {
    const { terminate, ...rest } = options ?? {};

    const batch = [] as any[];

    return this.$input.consume(
      (consumer, value) => {
        batch.push(value);
        if (batch.length < this.size) {
          consumer.next();
        } else {
          const array = [...batch];
          batch.length = 0;
          handler(consumer, array);
          array.length = 0;
        }
      },
      {
        ...rest,
        terminate(consumer, reason) {
          if (reason === "complete" && batch.length) {
            const array = [...batch];
            batch.length = 0;
            handler(consumer, array);
            array.length = 0;
          }
          batch.length = 0;
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function batch<INPUT extends Consumable.AnyConsumable>(size: number) {
  return (input: INPUT) => new Batch(input, size);
}

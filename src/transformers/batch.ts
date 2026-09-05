import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";

/**
 * Collects values into arrays of up to `size`. Unlike `buffer`, it does not use sliding windows.
 * Emits any remaining values as a partial batch on stream completion.
 *
 * @example
 * of(1,2,3,4,5).pipe(batch(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
 */
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
        }
      },
      {
        ...rest,
        terminate(consumer, reason) {
          if (reason === "complete" && batch.length) {
            const array = [...batch];
            batch.length = 0;
            handler(consumer, array);
          }
          batch.length = 0;
          terminate?.(consumer, reason);
        },
      },
    );
  }
}
/**
 * Collects values into arrays of up to `size`. Unlike `buffer`, it does not use sliding windows.
 * Emits any remaining values as a partial batch on stream completion.
 *
 * @param size Maximum number of values per batch.
 *
 * @example
 * of(1,2,3,4,5).pipe(batch(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
 */
export function batch<INPUT extends Consumable.AnyConsumable>(size: number) {
  return (input: INPUT) => new Batch(input, size);
}

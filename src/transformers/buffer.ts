import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable, SizedArray } from "../core/types";

/**
 * Collects values into fixed-size sliding-window arrays.
 * A new buffer starts every `startBufferEvery` values (defaults to `size`, i.e. non-overlapping).
 * Incomplete buffers are emitted on stream completion.
 *
 * @example
 * of(1,2,3,4,5).pipe(buffer(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
 */
export class Buffer<
  INPUT extends Consumable.AnyConsumable,
  SIZE extends number,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<SizedArray<VALUE, SIZE>> {
  constructor(
    private $input: INPUT,
    private size: SIZE,
    private startBufferEvery = size,
  ) {
    super();
  }
  consume(
    handler: Consumer.Handler<SizedArray<VALUE, SIZE>>,
    options?: Consumer.Options<SizedArray<VALUE, SIZE>>,
  ): Consumer<SizedArray<VALUE, SIZE>> {
    const { terminate, ...rest } = options ?? {};
    const { size, startBufferEvery } = this;

    const buffers: VALUE[][] = [];
    let count = 0;

    return this.$input.consume(
      (self, value) => {
        if (count++ % startBufferEvery === 0) buffers.push([]);

        for (const buffer of buffers) {
          buffer.push(value);
        }

        if (buffers[0].length === size) {
          handler(self, buffers.shift()! as SizedArray<VALUE, SIZE>);
        } else {
          self.next();
        }
      },
      {
        ...rest,
        terminate(consumer, reason) {
          if (reason === "complete") {
            buffers.forEach((buffer) => {
              if (buffer.length > 0) {
                handler(consumer, buffer as never);
              }
            });
          }
          buffers.length = 0;
          count = 0;
          terminate?.(consumer, reason);
        },
      },
    );
  }
}
/**
 * Collects values into fixed-size sliding-window arrays.
 * A new buffer starts every `startBufferEvery` values (defaults to `size`, i.e. non-overlapping).
 * Incomplete buffers are emitted on stream completion.
 *
 * @param size Number of values per buffer.
 * @param startBufferEvery How often to start a new buffer (default = `size`).
 *
 * @example
 * of(1,2,3,4,5).pipe(buffer(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
 */
export function buffer<INPUT extends Consumable.AnyConsumable, SIZE extends number>(
  size: SIZE,
  startBufferEvery = size,
) {
  return ($input: INPUT) => new Buffer($input, size, startBufferEvery);
}

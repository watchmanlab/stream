import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultQueue } from "../core/default-queue";
import { Source } from "../core/source";

import { ExtractValue, SizedArray } from "../core/types";

export class Buffer<
  INPUT extends Consumable.AnyConsumable,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
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

    const buffers = new DefaultQueue<VALUE[]>();
    let count = 0;

    return this.$input.consume(
      (self, value) => {
        if (count++ % this.startBufferEvery === 0) buffers.enqueue([]);

        let buffersSize = buffers.size;
        // we always have at most a single buffer full a time
        for (const buffer of buffers) {
          buffer.push(value);
          if (buffer.length === this.size) {
            buffers.dequeue(); //safe
            handler(self, [...buffer] as SizedArray<VALUE, SIZE>);
          }
        }

        if (buffersSize === buffers.size) self.next();
      },
      {
        ...rest,
        terminate(consumer, reason) {
          if (reason === "abort") {
            buffers.clear();
          } else {
            while (buffers.size > 0) {
              const buffer = buffers.dequeue() as VALUE[];

              if (buffer.length > 0) {
                handler(consumer, [...buffer] as never);
              }
            }
          }
          count = 0;
          terminate?.(consumer, reason);
        },
      },
    );
  }
}

export function buffer<INPUT extends Consumable.AnyConsumable, SIZE extends number>(
  size: SIZE,
  startBufferEvery = size,
) {
  return ($input: INPUT) => new Buffer($input, size, startBufferEvery);
}

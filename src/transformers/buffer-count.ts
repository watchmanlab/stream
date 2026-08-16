//@ts-nocheck
import { Consumer } from "../core/consumer";
import { DefaultQueue } from "../core/default-queue";
import { Source } from "../core/source";

import { AnyStream, ExtractValue, FixedArray, NonEmptyString, Transformer } from "../core/types";

export class BufferCount<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>
  extends Source<FixedArray<VALUE, SIZE>>
  implements Transformer<INPUT, FixedArray<VALUE, SIZE>>
{
  consume(
    handler: Consumer.Handler<FixedArray<VALUE, SIZE>>,
    options?: Consumer.Options<FixedArray<VALUE, SIZE>>,
  ): Consumer<FixedArray<VALUE, SIZE>> {}
  constructor(input: INPUT, size: SIZE, startBufferEvery = size) {
    const buffers = new DefaultQueue<VALUE[]>();
    let count = 0;

    consume: (handler, options) => {
      return input.consume(
        (self, value) => {
          if (count++ % startBufferEvery === 0) buffers.enqueue([]);

          let buffersSize = buffers.size;
          //since there is no reentrancy issue because of a safe queue mutation inside a loop ,
          // we always have a single buffer full at most at a time
          for (const buffer of buffers) {
            buffer.push(value);
            if (buffer.length === size) {
              buffers.dequeue(); //safe
              handler(self, [...buffer] as FixedArray<VALUE, SIZE>);
            }
          }

          if (buffersSize === buffers.size) self.next();
        },
        {
          ...options,
          events: {
            ...options?.events,
            abort: (self, value) => {
              buffers.clear();
              count = 0;
              options?.events?.abort?.(self, value);
            },

            complete: (self, value) => {
              while (buffers.size > 0) {
                const buffer = buffers.dequeue() as VALUE[];

                if (buffer.length > 0) {
                  handler(self, [...buffer] as never);
                }
              }
              count = 0;
              options?.events?.complete?.(self, value);
            },
          },
        },
      );
    };
  }
}

export function bufferCount<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
>(size: SIZE, startBufferEvery = size) {
  return ($input) => new BufferCount($input, size, startBufferEvery);
}

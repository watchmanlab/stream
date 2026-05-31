import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class BufferCount<
  INPUT extends Mitto.AnyMitto,
  SIZE extends number,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferCount.Name,
> extends Transformer<INPUT, Mitto.FixedArray<SIZE>, NAME> {
  readonly buffers = new Queue<VALUE[]>();
  constructor(name = bufferCount.NAME as NAME, input: INPUT, size: SIZE, startBufferEvery = size) {
    let count = 0;

    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (count % startBufferEvery === 0) {
            this.buffers.enqueue([]);
          }

          for (const buffer of this.buffers) {
            buffer.push(value);
            if (buffer.length === size) {
              this.emit([...buffer] as never);
              this.buffers.dequeue();
            }
          }

          count++;
        });

        return () => signal.emit();
      },
      aborted: () => {
        this.buffers.clear();
        count = 0;
      },
    });
  }
}

export function bufferCount<
  INPUT extends Mitto.AnyMitto,
  SIZE extends number,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferCount.Name,
>(size: SIZE, startBufferEvery = size): Mitto.Transform<INPUT, NAME, BufferCount<INPUT, SIZE, VALUE, NAME>> {
  return (input, name) => new BufferCount(name, input, size, startBufferEvery);
}
export namespace bufferCount {
  export const NAME = "bufferCount";
  export type Name = typeof NAME;
}

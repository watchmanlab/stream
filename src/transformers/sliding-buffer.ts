import type { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class SlidingBuffer<
  INPUT extends Mitto.AnyMitto,
  SIZE extends number,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = slidingBuffer.Name,
> extends Transformer<INPUT, Mitto.FixedArray<VALUE, SIZE>, NAME> {
  private _buffer = new Queue<VALUE>();
  constructor(name = slidingBuffer.NAME as NAME, input: INPUT, size: SIZE) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          this._buffer.enqueue(value);
          if (this._buffer.size > size) this._buffer.dequeue();
          if (this._buffer.size === size) this.emit([...this._buffer] as Mitto.FixedArray<VALUE, SIZE>);
        });
        return () => signal.emit();
      },
      aborted: () => this._buffer.clear(),
    });
  }
}
export function slidingBuffer<
  INPUT extends Mitto.AnyMitto,
  SIZE extends number,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = slidingBuffer.Name,
>(size: SIZE): Mitto.Transform<INPUT, NAME, SlidingBuffer<INPUT, SIZE, VALUE, NAME>> {
  return (input, name) => new SlidingBuffer(name, input, size);
}
export namespace slidingBuffer {
  export const NAME = "slidingBuffer";
  export type Name = typeof NAME;
}

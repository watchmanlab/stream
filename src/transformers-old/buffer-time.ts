import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class BufferTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferTime.Name,
> extends Transformer<INPUT, VALUE[], NAME> {
  private _buffer: VALUE[] = [];
  constructor(
    name = bufferTime.NAME as NAME,
    input: INPUT,
    public readonly ms: number,
  ) {
    let timer: any = null;

    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          this._buffer.push(value);

          if (!timer) {
            timer = setTimeout(() => {
              if (this._buffer.length > 0) {
                this.emit([...this._buffer]);
                this._buffer.length = 0;
              }
              timer = null;
            }, ms);
          }
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
        };
      },
      aborted: () => {
        clearTimeout(timer);
        this._buffer.length = 0;
      },
    });
  }
  get buffer() {
    return this._buffer.values();
  }
}

export function bufferTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferTime.Name,
>(ms: number): Mitto.Transform<INPUT, NAME, BufferTime<INPUT, VALUE, NAME>> {
  return (input, name) => new BufferTime(name, input, ms);
}
export namespace bufferTime {
  export const NAME = "bufferTime";
  export type Name = typeof NAME;
}

import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Group<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  SIZE extends number = 2,
  NAME extends string = group.Name,
> extends Transformer<INPUT, Mitto.FixedArray<VALUE, SIZE>, NAME> {
  private _buffer: VALUE[] = [];
  constructor(
    name = group.NAME as NAME,
    input: INPUT,
    public readonly size = 2 as SIZE,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          this._buffer.push(value);

          if (this._buffer.length === size) {
            const out = [...this._buffer];
            this._buffer.length = 0;
            this.emit(out as Mitto.FixedArray<VALUE, SIZE>);
          }
        });

        return () => signal.emit();
      },
      aborted: () => (this._buffer.length = 0),
    });
  }
  get buffer(): ArrayIterator<VALUE> {
    return this._buffer.values();
  }
}
export function group<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  SIZE extends number = 2,
  NAME extends string = group.Name,
>(size = 2 as SIZE): Mitto.Transform<INPUT, NAME, Group<INPUT, VALUE, SIZE, NAME>> {
  return (input, name) => new Group(name, input, size);
}
export namespace group {
  export const NAME = "group";
  export type Name = typeof NAME;
}

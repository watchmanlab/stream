import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Group<
  INPUT extends Mitto.AnyMitto,
  SIZE extends number = 2,
  NAME extends string = group.Name,
> extends Transformer<INPUT, Mitto.ExtractValue<INPUT>, NAME> {
  private _buffer = new Array();
  constructor(
    name = group.NAME as NAME,
    input: INPUT,
    public readonly size = 2 as SIZE,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          this._buffer.push(value);

          if (this._buffer.length === size) {
            const out = [...this._buffer];
            this._buffer.length = 0;
            this.emit(out as never);
          }
        });

        return () => signal.emit();
      },
      aborted: () => (this._buffer.length = 0),
    });
  }
  get buffer() {
    return this._buffer;
  }
}
export function group<INPUT extends Mitto.AnyMitto, SIZE extends number = 2, NAME extends string = group.Name>(
  size = 2 as SIZE,
): Mitto.Transform<INPUT, NAME, Group<INPUT, SIZE, NAME>> {
  return (input, name) => new Group(name, input, size);
}
export namespace group {
  export const NAME = "group";
  export type Name = typeof NAME;
}

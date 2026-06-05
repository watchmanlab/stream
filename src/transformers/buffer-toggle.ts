import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class BufferToggle<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferToggle.Name,
> extends Transformer<INPUT, VALUE[], NAME> {
  private _buffers = new Map<number, VALUE[]>();
  constructor(
    name = bufferToggle.NAME as NAME,
    input: INPUT,
    public readonly opening: Mitto.AnyMitto,
    public readonly closingSelector: () => Mitto.AnyMitto,
  ) {
    let id = 0;
    super(name, input, {
      scope: opening,
      source: () => {
        const s1 = input.listen((value) => {
          for (const buffer of this._buffers.values()) {
            buffer.push(value);
          }
        });

        const s2 = opening.listen(() => {
          const bufferId = id++;
          const buffer: VALUE[] = [];
          this._buffers.set(bufferId, buffer);

          const closing = closingSelector();
          closing.next(() => {
            this._buffers.delete(bufferId);
            this.emit([...buffer]);
            closing.abort();
          });
        });

        return () => {
          s1.emit();
          s2.emit();
        };
      },
      aborted: () => this._buffers.clear(),
    });
  }
  get buffers() {
    return this._buffers.values().map((buffer) => buffer.values());
  }
}

export function bufferToggle<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferToggle.Name,
>(
  opening: Mitto.AnyMitto,
  closingSelector: () => Mitto.AnyMitto,
): Mitto.Transform<INPUT, NAME, BufferToggle<INPUT, VALUE, NAME>> {
  return (input, name) => new BufferToggle(name, input, opening, closingSelector);
}

export namespace bufferToggle {
  export const NAME = "bufferToggle";
  export type Name = typeof NAME;
}

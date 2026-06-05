import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class BufferWhen<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferWhen.Name,
> extends Transformer<INPUT, VALUE[], NAME> {
  private _buffer: VALUE[] = [];
  constructor(
    name = bufferWhen.NAME as NAME,
    input: INPUT,
    public readonly notifier: Mitto.AnyMitto,
  ) {
    super(name, input, {
      scope: notifier,
      source: () => {
        const s1 = input.listen((value) => this._buffer.push(value));
        const s2 = notifier.listen(() => {
          if (this._buffer.length > 0) {
            this.emit([...this._buffer]);
            this._buffer.length = 0;
          }
        });

        return () => {
          s1.emit();
          s2.emit();
        };
      },
      aborted: () => (this._buffer.length = 0),
    });
  }
  get buffer() {
    return this._buffer.values();
  }
}

export function bufferWhen<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = bufferWhen.Name,
>(notifier: Mitto.AnyMitto): Mitto.Transform<INPUT, NAME, BufferWhen<INPUT, VALUE, NAME>> {
  return (input, name) => new BufferWhen(name, input, notifier);
}

export namespace bufferWhen {
  export const NAME = "bufferWhen";
  export type Name = typeof NAME;
}

import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Emit<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = emit.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _values: [value: VALUE, ...values: VALUE[]];
  constructor(name = emit.NAME as NAME, input: INPUT, values: [value: VALUE, ...values: VALUE[]]) {
    super(name, input, {
      source: () => {
        this.emit(...this._values);
        this._values.length = 0;

        const signal = input.listen((value) => this.emit(value));

        return () => signal.emit();
      },
    });
    this._values = [...values];
  }
  get values(): ArrayIterator<VALUE> {
    return this._values.values();
  }
}

export function emit<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = emit.Name,
>(...values: [value: VALUE, ...values: VALUE[]]): Mitto.Transform<INPUT, NAME, Emit<INPUT, VALUE, NAME>> {
  return (input, name) => new Emit(name, input, values);
}

export namespace emit {
  export const NAME = "emit";
  export type Name = typeof NAME;
}

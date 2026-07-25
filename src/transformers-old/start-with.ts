import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class StartWith<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = startWith.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _values: VALUE[];
  constructor(name = startWith.NAME as NAME, input: INPUT, values: VALUE[]) {
    super(name, input, {
      source: () => {
        const signal = input.consume((v) => this.emit(v));

        return () => signal.emit();
      },
      listenerAdded: (listener) => {
        for (const value of this._values) listener(value);
      },
    });
    this._values = [...values];
  }

  get values() {
    return this._values.values();
  }
}

export function startWith<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = startWith.Name,
>(...values: [value: VALUE, ...values: VALUE[]]): Mitto.Transform<INPUT, NAME, StartWith<INPUT, VALUE, NAME>> {
  return (input, name) => new StartWith(name, input, values);
}

export namespace startWith {
  export const NAME = "startWith";
  export type Name = typeof NAME;
  export type Fn<VALUE> = (value: VALUE) => void;
}

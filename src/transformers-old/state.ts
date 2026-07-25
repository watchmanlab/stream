import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class State<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = state.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _value: VALUE;
  constructor(
    name = state.NAME as NAME,
    input: INPUT,
    public readonly initialValue: VALUE,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => this.set(value));

        return () => signal.emit();
      },
    });

    this._value = initialValue;
  }

  get() {
    return this._value;
  }

  set(value: VALUE) {
    this._value = value;
    this.emit(value);
  }
}

export function state<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = state.Name,
>(initialValue: VALUE): Mitto.Transform<INPUT, NAME, State<INPUT, VALUE, NAME>> {
  return (input, name) => new State(name, input, initialValue);
}

export namespace state {
  export const NAME = "state";
  export type Name = typeof NAME;
}

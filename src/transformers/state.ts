import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class State<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = state.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _value: VALUE;
  constructor(options: state.Options<INPUT, VALUE, NAME>) {
    super({
      name: options.name ?? (state.NAME as NAME),
      input: options.input,
      source: () => options.input.listen((value) => (this.value = value)).emit.bind(this),
    });

    this._value = options.initialValue;
  }

  get value() {
    return this._value;
  }

  set value(value: VALUE) {
    this._value = value;
    this.emit(value);
  }
}

export function state<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = state.Name,
>(initialValue: VALUE): Mitto.Transform<INPUT, NAME, State<INPUT, VALUE, NAME>> {
  return (input, name) => new State({ initialValue, input, name });
}

export namespace state {
  export const NAME = "state";
  export type Name = typeof NAME;

  export type Options<INPUT extends Mitto.AnyMitto, VALUE, NAME extends string> = {
    name?: NAME;
    input: INPUT;
    initialValue: VALUE;
  };
}

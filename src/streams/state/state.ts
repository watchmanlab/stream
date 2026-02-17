import { Stream } from "..";

const NAME = "stated";
type Name = typeof NAME;
export class State<VALUE> extends Stream<VALUE, Name> {
  protected _value: VALUE;
  protected _options: State.Options = { emitCurrent: false };

  constructor(initialValue: VALUE, options?: State.Options) {
    super(NAME);

    this._value = initialValue;
    this.options = options ?? {};
  }

  override push(value: VALUE, ...values: VALUE[]): Promise<void> {
    this._value = values.length ? values[values.length - 1] : value;
    return super.push(value, ...values);
  }

  get value() {
    return this._value;
  }
  set value(newValue: VALUE) {
    if (this._value === newValue) return;
    this._value = newValue;
    super.push(newValue);
  }
  get options() {
    return this._options;
  }
  set options(options: State.Options) {
    this._options = { ...this._options, ...options };
  }
}

export namespace State {
  export type Options = {
    emitCurrent?: boolean; // Default: false
  };
}

const state = new State(99, { emitCurrent: true });

state.listen((v) => console.log("s1", v));
state.listen((v) => console.log("s2", v));
// state.listen((v) => console.log("s3", v));

state.push(1, 2, 3);

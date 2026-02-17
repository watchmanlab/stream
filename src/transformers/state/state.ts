import { Stream } from "../../streams/stream/stream";

const NAME = "stated";
type Name = typeof NAME;
class State<VALUE, NAME extends string = Name> extends Stream<VALUE, NAME> {
  protected _value: VALUE;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, initialValue: VALUE, options?: state.Options) {
    const { emitCurrent = false } = options ?? {};

    super(name, async function* () {
      if (emitCurrent) yield self._value;
    });

    const self = this;

    this._value = initialValue;

    // HOT: Start consuming source immediately
    source.listen((value) => {
      if (this._value === value) return;
      this._value = value;
      this.push(value);
    });
  }

  get value() {
    return this._value;
  }

  set value(newValue: VALUE) {
    if (this._value === newValue) return;
    this._value = newValue;
    this.push(newValue);
  }
}

export function state<VALUE, NAME extends string = Name>(
  initialValue: VALUE,
  options?: state.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, State<VALUE, NAME>> {
  return (_, source, name) => new State(source, name, initialValue, options);
}

export namespace state {
  export type Options = {
    emitCurrent?: boolean; // Default: false
  };
}

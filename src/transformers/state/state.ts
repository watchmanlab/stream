import { Stream } from "../../stream";

const NAME = "stated";

class State<VALUE, NAME extends string = state.Name> extends Stream<VALUE, NAME> {
  protected _value: VALUE;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, initialValue: VALUE) {
    super(name, async function* () {
      try {
        for await (const value of source) {
          if (self._value === value) continue;
          self._value = value;
          yield value;
        }
      } finally {
        return;
      }
    });

    const self = this;
    this._value = initialValue;
  }

  get value() {
    return this._value;
  }
  set value(value) {
    if (this._value === value) return;
    this._value = value;
    this.push(value);
  }
}

export function state<VALUE, NAME extends string = state.Name>(
  initialValue: VALUE,
): Stream.Transformer<NAME, Stream<VALUE, any>, State<VALUE, NAME>> {
  return (_, source, name) => new State(source, name, initialValue);
}

export namespace state {
  export type Name = typeof NAME;
}

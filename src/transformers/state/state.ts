import { Stream } from "../../stream";
import { consumer } from "../consumer";

const NAME = "stated";

class State<VALUE, NAME extends string = state.Name> extends Stream<VALUE, NAME> {
  protected _value: VALUE;
  protected _options: state.Options = { emitCurrent: false };
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, initialValue: VALUE, options?: state.Options) {
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
    this.options = options ?? {};
    if (this._options.emitCurrent) {
      this._onConsumerJoined = (send) => send(this._value);
    }
  }

  get value() {
    return this._value;
  }
  set value(value) {
    if (this._value === value) return;
    this._value = value;
    this.push(value);
  }
  get options() {
    return this._options;
  }
  set options(options) {
    this._options = { ...this._options, ...options };
  }
}

export function state<VALUE, NAME extends string = state.Name>(
  initialValue: VALUE,
  options?: state.Options,
): Stream.Transformer<NAME, Stream<VALUE, any>, State<VALUE, NAME>> {
  return (_, source, name) => new State(source, name, initialValue, options);
}

export namespace state {
  export type Name = typeof NAME;
  export type Options = {
    emitCurrent?: boolean;
  };
}

const stream = new Stream<number>().pipe(state(9, { emitCurrent: true })).pipe(consumer((value) => console.log(value)));

stream.sss.sss.push(33);

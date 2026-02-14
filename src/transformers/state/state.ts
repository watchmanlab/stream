import { Stream } from "../../stream";
export class State<VALUE, NAME extends string = State.Name> extends Stream<VALUE, NAME> {
  protected _value: VALUE;
  protected _events?: Stream<State.Event<VALUE>, "Event">;

  constructor(source: Stream<VALUE, any>, initialValue: VALUE, options?: State.Options<NAME>) {
    const { name = State.NAME as NAME, emitCurrent = false } = options ?? {};

    let currentValue = initialValue;

    super(name, async function* () {
      // Optionally emit current value to new consumers
      if (emitCurrent) yield currentValue;

      // Then receive updates
      for await (const value of source) {
        if (currentValue === value) continue;

        const oldValue = currentValue;
        currentValue = value;
        self._value = value;
        self._events?.push({ type: "changed", from: oldValue, to: value });
        yield value;
      }
    });

    const self = this;
    this._value = currentValue;

    // HOT: Start consuming source immediately
    source.listen((value) => {
      if (this._value === value) return;
      const oldValue = this._value;
      this._value = value;
      this._events?.push({ type: "changed", from: oldValue, to: value });
      this.push(value);
    });
  }

  get value() {
    return this._value;
  }

  set value(newValue: VALUE) {
    if (this._value === newValue) return;
    const oldValue = this._value;
    this._value = newValue;
    this._events?.push({ type: "changed", from: oldValue, to: newValue });
    this.push(newValue);
  }

  get events() {
    if (!this._events) this._events = new Stream();
    return this._events;
  }
}

export namespace State {
  export const NAME = "stated";
  export type Name = typeof NAME;
  export type Options<NAME extends string> = {
    name?: NAME;
    emitCurrent?: boolean; // Default: false
  };
  export type Event<VALUE> = {
    type: "changed";
    from: VALUE;
    to: VALUE;
  };
}

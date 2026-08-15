import { Stream } from "../core/stream";

export class State<VALUE> extends Stream<VALUE> {
  private _value: VALUE;

  constructor(initialValue: VALUE) {
    super();
    this._value = initialValue;
  }
  get value(): VALUE {
    return this._value;
  }
  set value(v: VALUE) {
    this._value = v;
    this.push(v);
  }
}

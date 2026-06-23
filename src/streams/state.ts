import { Stream, type stream } from "../core/stream";

export class State<VALUE, NAME extends string = "state"> extends Stream<VALUE, NAME> {
  private _value: VALUE;

  constructor(initialValue: VALUE, init?: stream.Init<VALUE, NAME>) {
    super({ ...init, name: init?.name ?? ("state" as NAME) });
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

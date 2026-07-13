import { Stream } from "./stream";
import { NonEmptyString } from "./types";

export class State<VALUE, NAME extends NonEmptyString = "state"> extends Stream<VALUE, NAME> {
  private _value: VALUE;

  constructor(initialValue: VALUE, options?: Stream.Options<VALUE, NAME>) {
    super({ ...options, name: options?.name ?? ("state" as NAME) });
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

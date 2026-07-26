import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class State<VALUE, NAME extends NonEmptyString = "$state"> extends Stream<VALUE, NAME> {
  private _value: VALUE;

  constructor(initialValue: VALUE, options?: Stream.Options<VALUE, NAME>) {
    super({ ...options, name: options?.name ?? ("$state" as NAME) });
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

export function state<VALUE, NAME extends NonEmptyString = "$state">(
  initialValue: VALUE,
  options?: Stream.Options<VALUE, NAME>,
): State<VALUE, NAME> {
  return new State(initialValue, options);
}

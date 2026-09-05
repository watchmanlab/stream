import { Stream } from "../core/stream";

/**
 * A `Stream` with a readable/writable `.value` property.
 * Setting `.value` pushes the new value to all consumers.
 *
 * @example
 * const count = state(0);
 * count.pipe(listen(v => console.log('count:', v)));
 * count.value = 1; // logs 'count: 1'
 */
export class State<VALUE> extends Stream<VALUE> {
  private _value: VALUE;

  constructor(initialValue: VALUE, options?: Stream.Options<VALUE>) {
    super(options);
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

/**
 * Creates a `State` stream with an initial value.
 * @param initialValue The initial state value.
 * @param options The Stream options.
 */
export function state<VALUE>(initialValue: VALUE, options?: Stream.Options<VALUE>) {
  return new State(initialValue, options);
}

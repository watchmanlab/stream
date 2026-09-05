import { Stream } from "../core/stream";

/**
 * A `Stream` with a readable/writable `.value` property.
 * Setting `.value` pushes the new value to all consumers.
 * `NOTE`: {@link push} notify all the consumers without
 *  changing the value property,this is intentional,and allow for more patterns
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
 * A `Stream` with a readable/writable `.value` property.
 * Setting `.value` pushes the new value to all consumers.
 * `NOTE`: {@link push} notify all the consumers without
 *  changing the value property,this is intentional,and allow for more patterns
 *
 *
 * @param initialValue The initial state value.
 * @param options The Stream options.
 *
 * @example
 * const count = state(0);
 * count.pipe(listen(v => console.log('count:', v)));
 * count.value = 1; // logs 'count: 1'
 */

export function state<VALUE>(initialValue: VALUE, options?: Stream.Options<VALUE>) {
  return new State(initialValue, options);
}

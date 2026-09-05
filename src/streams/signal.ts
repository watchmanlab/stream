import { Stream } from "../core/stream";

/**
 * A `Stream` that automatically terminates after the first `push`.
 * Useful for one-shot events or signals.
 *
 * @example
 * const signal = new Signal<string>();
 * signal.pipe(listen(console.log));
 * signal.push('done'); // emits 'done' then terminates
 */
export class Signal<VALUE> extends Stream<VALUE> {
  override push(value: VALUE): this {
    super.push(value);
    this.terminate("complete");
    return this;
  }
}

/**
 * Creates a `Signal` stream.
 * @param options The Stream options.
 */
export function signal<VALUE>(options?: Stream.Options<VALUE>) {
  return new Signal(options);
}

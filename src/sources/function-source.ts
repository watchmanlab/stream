import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * `Replayable`
 *
 * Calls `fn` once, emit the result then completes.
 *
 * @example
 * fromFunction(() => Math.random()).pipe(listen(console.log));
 */
export class FunctionSource<VALUE> extends Source<VALUE> {
  constructor(private fn: (...args: any[]) => VALUE) {
    super();
  }

  override consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    const { next, ...rest } = options ?? {};
    const { fn } = this;

    return new Consumer(
      (c, v) => {
        c.terminate("complete");
        handler(c, v);
      },
      {
        ...rest,
        next(c) {
          c.push(fn());
          next?.(c);
        },
      },
    );
  }
}
/**
 * `Replayable`
 *
 * Calls `fn` once, emit the result then completes.
 *
 * @param fn Function called once to produce the emitted value.
 *
 * @example
 * fromFunction(() => Math.random()).pipe(listen(console.log));
 */
export function fromFunction<VALUE>(fn: (...args: any[]) => VALUE): FunctionSource<VALUE> {
  return new FunctionSource(fn);
}

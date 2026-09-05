import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Calls `fn` once per `next()` credit and emits the return value, then completes.
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
 * Creates a `FunctionSource`.
 * @param fn Function called once to produce the emitted value.
 */
export function fromFunction<VALUE>(fn: (...args: any[]) => VALUE): FunctionSource<VALUE> {
  return new FunctionSource(fn);
}

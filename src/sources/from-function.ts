import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

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

export function fromFunction<VALUE>(fn: (...args: any[]) => VALUE): FunctionSource<VALUE> {
  return new FunctionSource(fn);
}

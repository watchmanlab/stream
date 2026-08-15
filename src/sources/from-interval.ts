import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class FromInterval<MS extends number> extends Source<void> {
  constructor(private ms: MS) {
    super();
  }
  consume(handler: Consumer.Handler<void>, options?: Consumer.Options<void>): Consumer<void> {
    const { init, ...rest } = options ?? {};
    return new Consumer(handler, {
      ...rest,
      init: (consumer) => {
        const timer = setInterval(() => consumer.push(), this.ms);

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          clearInterval(timer);
        };
      },
    });
  }
}

export function fromInterval<MS extends number>(ms: MS): FromInterval<MS> {
  return new FromInterval(ms);
}

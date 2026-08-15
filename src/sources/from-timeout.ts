import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class FromTimeout<MS extends number> extends Source<void> {
  constructor(private ms: MS) {
    super();
  }

  consume(handler: Consumer.Handler<void>, options?: Consumer.Options<void>): Consumer<void> {
    const { init, ...rest } = options ?? {};

    return new Consumer(handler, {
      ...rest,

      init: (consumer) => {
        const timer = setTimeout(() => {
          consumer.push();
          consumer.terminate("complete");
        }, this.ms);

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          clearTimeout(timer);
        };
      },
    });
  }
}

export function fromTimeout<MS extends number>(ms: MS): FromTimeout<MS> {
  return new FromTimeout(ms);
}

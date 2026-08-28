import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

export class TimeoutSource<MS extends number, VALUE = void> extends Source<VALUE> {
  constructor(
    private ms: MS,
    private value?: VALUE,
  ) {
    super();
  }

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { init, ...rest } = options ?? {};
    const { ms, value } = this;

    return new Consumer(handler, {
      ...rest,

      init: (consumer) => {
        const timer = setTimeout(() => {
          consumer.push(value as VALUE);
          consumer.terminate("complete");
        }, ms);

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          clearTimeout(timer);
        };
      },
    });
  }
}

export function fromTimeout<MS extends number, VALUE = void>(ms: MS, value?: VALUE): TimeoutSource<MS, VALUE> {
  return new TimeoutSource(ms, value);
}

import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export function fromInterval(ms: number): Source<void> {
  return {
    consume(handler, options) {
      const { init, ...rest } = options ?? {};
      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          const timer = setInterval(() => consumer.push(), ms);

          const cleanup = init?.(consumer);

          return (reason) => {
            cleanup?.(reason);
            clearInterval(timer);
          };
        },
      });
    },
  };
}

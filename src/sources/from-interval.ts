import { Consumer } from "../core/consumer";
import { Consumable } from "../core/types";

export function fromInterval(ms: number): Consumable<void> {
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

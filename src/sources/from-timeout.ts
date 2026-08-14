import { Consumer } from "../core/consumer";
import { Consumable } from "../core/types";

export function fromTimeout(ms: number): Consumable<void> {
  return {
    consume(handler, options) {
      const { init, ...rest } = options ?? {};

      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          const timer = setTimeout(() => {
            consumer.push();
            consumer.terminate("complete");
          }, ms);

          const cleanup = init?.(consumer);

          return (reason) => {
            cleanup?.(reason);
            clearTimeout(timer);
          };
        },
      });
    },
  };
}

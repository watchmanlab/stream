import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export function fromGCToken(token: object): Source<void> {
  const ref = new WeakRef(token);
  const unregisterToken = {};
  let registry: FinalizationRegistry<unknown> | undefined;

  return {
    consume(handler, options) {
      const { init, ...rest } = options ?? {};
      return new Consumer(handler, {
        ...rest,
        init(consumer) {
          const obj = ref.deref();
          if (!obj) {
            consumer.push();
            consumer.terminate("complete");
          } else {
            registry = new FinalizationRegistry(() => {
              consumer.push();
              consumer.terminate("complete");
            });

            registry.register(obj, undefined, unregisterToken);
          }

          const cleanup = init?.(consumer);

          return (reason) => {
            cleanup?.(reason);
            registry?.unregister(unregisterToken);
          };
        },
      });
    },
  };
}

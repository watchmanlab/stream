import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class GCSignalStream<VALUE extends void, NAME extends NonEmptyString> extends Stream<void, NAME> {
  constructor(token: object, options?: GCSignalStream.Options<NAME>) {
    const ref = new WeakRef(token);
    const unregisterToken = {};

    super({
      ...options,
      source: {
        listen: (handler, options) => {
          let registry: FinalizationRegistry<unknown> | undefined;

          new Promise<void>((resolve) => {
            if (!ref.deref()) {
              resolve();
              return;
            }
            registry = new FinalizationRegistry(resolve);

            const obj = ref.deref();
            if (obj) {
              registry.register(obj, undefined, unregisterToken);
            } else {
              resolve();
            }
          })
            .then(() => consumer.push())
            .catch((error) => consumer.abort(error))
            .finally(() => {
              consumer.complete();
              registry?.unregister(unregisterToken);
            });

          const consumer = new Consumer<void, any>(handler, {
            ...options,
            events: {
              ...options?.events,
              abort(self, error) {
                registry?.unregister(unregisterToken);
                options?.events?.abort?.(self, error);
              },
              complete(self) {
                registry?.unregister(unregisterToken);
                options?.events?.complete?.(self);
              },
            },
          });
          return consumer;
        },
      },
    });
  }
}

export namespace GCSignalStream {
  export type Options<NAME extends NonEmptyString> = Omit<Stream.Options<void, NAME>, "source">;
}

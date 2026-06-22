import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

const NAME = "gc-signal-stream";

export class GCSignalStream<VALUE extends void, NAME extends string> extends Stream<void, NAME> {
  constructor(token: object, init?: Omit<Stream.Init<void, NAME>, "source">) {
    const ref = new WeakRef(token);
    const unregisterToken = {};

    super({
      ...init,
      source: {
        listen: (init) => {
          let registry: FinalizationRegistry<unknown> | undefined;

          new Promise<void>((resolve) => {
            if (!ref.deref()) {
              consumer.push();
              return consumer;
            }
            registry = new FinalizationRegistry(() => {
              resolve();
            });

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

          const consumer = new Consumer<void, any>(init);
          return consumer;
        },
      },
    });
  }
}

export namespace GCSignalStream {}

import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class GCSignalStream<NAME extends NonEmptyString = "$gcSignal"> extends Stream<void, NAME> {
  constructor(token: object, options?: Stream.Options<void, NAME>) {
    const ref = new WeakRef(token);
    const unregisterToken = {};
    let registry: FinalizationRegistry<unknown> | undefined;

    super({
      ...options,
      name: options?.name ?? ("$gcSignal" as NAME),
      next(stream, consumer) {
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
          .then(() => stream.push())
          .finally(() => {
            registry?.unregister(unregisterToken);
            stream.terminate("complete");
          });
        options?.next?.(stream, consumer);
      },
      terminate(stream, reason) {
        registry?.unregister(unregisterToken);
        options?.terminate?.(stream, reason);
      },
    });
  }
}

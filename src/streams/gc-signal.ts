import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class GCSignal<NAME extends NonEmptyString = "$gcSignal"> extends Stream<void, NAME> {
  constructor(token: object, options?: Stream.Options<void, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const ref = new WeakRef(token);
    const unregisterToken = {};
    let registry: FinalizationRegistry<unknown> | undefined;

    super({
      ...rest,
      name: name ?? ("$gcSignal" as NAME),
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
        next?.(stream, consumer);
      },
      terminate(stream, reason) {
        registry?.unregister(unregisterToken);
        terminate?.(stream, reason);
      },
    });
  }
}

export function gcSignal<NAME extends NonEmptyString = "$gcSignal">(
  token: object,
  options?: Stream.Options<void, NAME>,
): GCSignal<NAME> {
  return new GCSignal(token, options);
}

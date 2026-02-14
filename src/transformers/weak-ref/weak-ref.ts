import { Stream } from "../../stream";
import { merge } from "../merge";

export function weakRef<VALUE>(object: object): Stream.Transformer<Stream<VALUE>, Stream<Controller.Aborted>> {
  const ref = new WeakRef(object);
  const unregisterToken = {};
  return function (source) {
    return new Stream(async function* () {
      for await (const _ of source) {
        yield Controller.ABORTED;
      }
    }).pipe(
      merge(
        new Stream(async function* () {
          let registry: FinalizationRegistry<unknown> | undefined;
          try {
            if (!ref.deref()) {
              yield Controller.ABORTED;
              return;
            }

            yield new Promise<Controller.Aborted>((resolve) => {
              registry = new FinalizationRegistry(() => {
                resolve(Controller.ABORTED);
              });

              const obj = ref.deref();
              if (obj) {
                registry.register(obj, undefined, unregisterToken);
              } else {
                resolve(Controller.ABORTED);
              }
            });
          } finally {
            registry?.unregister(unregisterToken);
          }
        }),
      ),
    );
  };
}

import { Stream, Controller } from "../../stream";
import { merge } from "../merge";

/**
 * Emits ABORTED when object is garbage collected.
 * Useful for automatic cleanup based on object lifetime.
 *
 * @example
 * ```typescript
 * const element = document.createElement('div');
 *
 * stream
 *   .pipe(weakRef(element))
 *   .listen(value => {
 *     if (value === Stream.Controller.ABORTED) {
 *       console.log('Element GC\'d');
 *     } else {
 *       element.textContent = String(value);
 *     }
 *   });
 * ```
 */
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

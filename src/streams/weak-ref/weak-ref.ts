import { Stream } from "..";

const NAME = "weak-ref";

type Name = typeof NAME;

export class WeakRef extends Stream<WeakRef.Signal, Name> {
  constructor(token: object) {
    const ref = new globalThis.WeakRef(token);
    const unregisterToken = {};

    super(NAME, async function* () {
      let registry: FinalizationRegistry<unknown> | undefined;
      try {
        if (!ref.deref()) {
          yield WeakRef.SIGNAL;
          return;
        }

        yield new Promise<WeakRef.Signal>((resolve) => {
          registry = new FinalizationRegistry(() => {
            resolve(WeakRef.SIGNAL);
          });

          const obj = ref.deref();
          if (obj) {
            registry.register(obj, undefined, unregisterToken);
          } else {
            resolve(WeakRef.SIGNAL);
          }
        });
      } finally {
        registry?.unregister(unregisterToken);
      }
    });
  }
}

export namespace WeakRef {
  export const SIGNAL = Symbol("*weak-ref-signal#");
  export type Signal = typeof SIGNAL;
}

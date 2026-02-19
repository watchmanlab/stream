import { Stream } from "../../stream";

const NAME = "gc-signal";
type Name = typeof NAME;

export class GCSignal extends Stream<GCSignal.Signal, Name> {
  protected _garbageCollected = false;
  constructor(token: object) {
    const ref = new WeakRef(token);
    const unregisterToken = {};

    super(NAME, async function* () {
      let registry: FinalizationRegistry<unknown> | undefined;
      try {
        if (!ref.deref()) {
          yield GCSignal.SIGNAL;
          return;
        }

        yield new Promise<GCSignal.Signal>((resolve) => {
          registry = new FinalizationRegistry(() => {
            resolve(GCSignal.SIGNAL);
          });

          const obj = ref.deref();
          if (obj) {
            registry.register(obj, undefined, unregisterToken);
          } else {
            resolve(GCSignal.SIGNAL);
          }
        });
      } finally {
        self._garbageCollected = true;
        registry?.unregister(unregisterToken);
      }
    });
    const self = this;
  }
  get garbageCollected() {
    return this._garbageCollected;
  }
}

export namespace GCSignal {
  export const SIGNAL = Symbol("*gc-signal#");
  export type Signal = typeof SIGNAL;
}

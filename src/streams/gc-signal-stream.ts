import { Stream } from "./stream";

const NAME = "gc-signal-stream";

export class GCSignalStream<NAME extends string = GCSignalStream.Name> extends Stream<GCSignalStream.Signal, NAME> {
  protected _garbageCollected = false;
  constructor(token: object, name = NAME as NAME) {
    const ref = new WeakRef(token);
    const unregisterToken = {};

    super(name, async function* () {
      let registry: FinalizationRegistry<unknown> | undefined;
      try {
        if (!ref.deref()) {
          yield GCSignalStream.SIGNAL;
          return;
        }

        yield new Promise<GCSignalStream.Signal>((resolve) => {
          registry = new FinalizationRegistry(() => {
            resolve(GCSignalStream.SIGNAL);
          });

          const obj = ref.deref();
          if (obj) {
            registry.register(obj, undefined, unregisterToken);
          } else {
            resolve(GCSignalStream.SIGNAL);
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

export namespace GCSignalStream {
  export type Name = typeof NAME;
  export const SIGNAL = Symbol(`*${NAME}#`);
  export type Signal = typeof SIGNAL;
}

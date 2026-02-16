import { Stream } from "../../stream";
import { merge } from "../merge";

const NAME = "weak-ref";

type Name = typeof NAME;

class WeakRef<VALUE, NAME extends string = Name> extends Stream<VALUE | weakRef.Signal, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, token: object) {
    const ref = new globalThis.WeakRef(token);
    const unregisterToken = {};

    const fn = async function* () {
      let registry: FinalizationRegistry<unknown> | undefined;
      try {
        if (!ref.deref()) {
          yield weakRef.SIGNAL;
          return;
        }

        yield new Promise<weakRef.Signal>((resolve) => {
          registry = new FinalizationRegistry(() => {
            resolve(weakRef.SIGNAL);
          });

          const obj = ref.deref();
          if (obj) {
            registry.register(obj, undefined, unregisterToken);
          } else {
            resolve(weakRef.SIGNAL);
          }
        });
      } finally {
        registry?.unregister(unregisterToken);
      }
    };

    super(name, source.pipe(merge(fn())) as Stream<any, any>);
  }
}
export function weakRef<VALUE, NAME extends string = Name>(
  token: object,
): Stream.Transformer<NAME, Stream<VALUE, any>, WeakRef<VALUE, NAME>> {
  return (_, source, name) => new WeakRef(source, name, token);
}

export namespace weakRef {
  export const SIGNAL = Symbol("*weak-ref-signal#");
  export type Signal = typeof SIGNAL;
}

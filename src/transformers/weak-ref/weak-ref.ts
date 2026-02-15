import { Stream } from "../../stream";
import { merge } from "../merge";

export class WeakRef<NAME extends string = WeakRef.Name> extends Stream<Stream.Abort, NAME> {
  constructor(object: object, options?: WeakRef.Options<NAME>) {
    const { name = WeakRef.NAME as NAME } = options ?? {};
    const ref = new globalThis.WeakRef(object);
    const unregisterToken = {};

    super(name, async function* () {
      let registry: FinalizationRegistry<unknown> | undefined;
      try {
        if (!ref.deref()) {
          yield Stream.ABORT;
          return;
        }

        yield new Promise<Stream.Abort>((resolve) => {
          registry = new FinalizationRegistry(() => {
            resolve(Stream.ABORT);
          });

          const obj = ref.deref();
          if (obj) {
            registry.register(obj, undefined, unregisterToken);
          } else {
            resolve(Stream.ABORT);
          }
        });
      } finally {
        registry?.unregister(unregisterToken);
      }
    });
  }
}

export function weakRef<SOURCE extends Stream<any, any>, NAME extends string = WeakRef.Name>(object: object) {
  return (source: SOURCE, name: Stream.NameOf<SOURCE>) => source.pipe(merge(new WeakRef<NAME>(object, { name })));
}

export namespace WeakRef {
  export const NAME = "weak-ref";
  export type Name = typeof NAME;

  export type Options<NAME extends string> = {
    name?: NAME;
  };
}
const s = new Stream<number>();

const res = s.pipe(merge(new WeakRef({})));

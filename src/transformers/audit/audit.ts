import { Stream } from "../../streams/index.ts";

const NAME = "audit";

export class Audit<SOURCE extends Stream<any, any>, NAME extends string = audit.Name> extends Stream<
  Stream.ExtractValue<SOURCE>,
  NAME
> {
  constructor(source: SOURCE, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let canEmit = true;

      try {
        for await (const value of source) {
          if (Stream.isSentinel(value)) {
            yield value;
            continue;
          }

          if (!canEmit) continue;
          canEmit = false;
          clearTimeout(timer);
          timer = setTimeout(() => (canEmit = true), ms);
          yield value;
        }
      } finally {
        clearTimeout(timer);
      }
    });
  }
}

export function audit<SOURCE extends Stream<any, any>, NAME extends string = audit.Name>(
  ms: number,
): Stream.Transform<NAME, SOURCE, Audit<SOURCE, NAME>> {
  return (_, source, name) => new Audit(source, name, ms);
}

export namespace audit {
  export type Name = typeof NAME;
}

import { Stream } from "../../streams/index.ts";

const NAME = "audit";

export class Audit<VALUE, NAME extends string = audit.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();
      let timer: any = null;
      let canEmit = true;

      try {
        while (!result.done) {
          let error: Stream.MaybeSourceError;
          if (canEmit) {
            canEmit = false;
            clearTimeout(timer);
            timer = setTimeout(() => (canEmit = true), ms);
            error = yield result.value;
          }
          result = await generator.next(error);
        }
      } finally {
        clearTimeout(timer);
        await generator.return();
      }
    });
  }
}

export function audit<VALUE, NAME extends string = audit.Name>(
  ms: number,
): Stream.Transformer<NAME, Stream<VALUE, any>, Audit<VALUE, NAME>> {
  return (_, source, name) => new Audit(source, name, ms);
}

export namespace audit {
  export type Name = typeof NAME;
}

const r = new Stream<number>().pipe(audit(40));

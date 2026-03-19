import { Stream } from "../../streams/index.ts";

const NAME = "debounce";

export class Debounce<
  SOURCE extends Stream<any, any>,
  VALUE extends Stream.ExtractValue<SOURCE> = Stream.ExtractValue<SOURCE>,
  NAME extends string = debounce.Name,
> extends Stream<VALUE, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let aborted = false;

      let resolve: (value: VALUE) => void = () => {};
      const generator = source[Symbol.asyncIterator]();

      (async () => {
        for await (const value of generator) {
          if (Stream.isSentinel(value)) {
            resolve(value as never);
            continue;
          }
          clearTimeout(timer);
          if (aborted) {
            resolve(value);
            break;
          }
          timer = setTimeout(() => resolve(value), ms);
        }
      })();

      try {
        while (true) {
          yield await new Promise<VALUE>((r) => (resolve = r));
        }
      } finally {
        clearTimeout(timer);
        aborted = true;
        await generator.return();
      }
    });
  }
}

export function debounce<
  SOURCE extends Stream<any, any>,
  VALUE extends Stream.ExtractValue<SOURCE> = Stream.ExtractValue<SOURCE>,
  NAME extends string = debounce.Name,
>(ms: number): Stream.Transformer<NAME, SOURCE, Debounce<SOURCE, VALUE, NAME>> {
  return (_, source, name) => new Debounce(source, name, ms);
}

export namespace debounce {
  export type Name = typeof NAME;
}

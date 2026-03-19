import { Stream } from "../../streams/index.ts";

const NAME = "distinct";

export class Distinct<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = distinct.Name,
> extends Stream<Stream.ExtractValue<SOURCE>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME) {
    super(name, async function* () {
      const seen = new Set<CLEAN_VALUE>();

      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }
        if (seen.has(value)) continue;
        seen.add(value);
        yield value;
      }
    });
  }
}

export function distinct<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = distinct.Name,
>(): Stream.Transformer<NAME, SOURCE, Distinct<SOURCE, CLEAN_VALUE, NAME>> {
  return (_, source, name) => new Distinct(source, name);
}

export namespace distinct {
  export type Name = typeof NAME;
}

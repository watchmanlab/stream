import { Stream } from "../../streams/index.ts";

const NAME = "flat";

export class Flat<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
> extends Stream<FlatArray<CLEAN_VALUE, DEPTH> | Stream.ExtractSentinel<SOURCE>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, depth = 0 as DEPTH) {
    super(name, async function* () {
      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value as never;
          continue;
        }
        if (Array.isArray(value)) {
          const values = value.flat(depth);
          for (let i = 0; i < values.length; i++) {
            yield values[i]!;
          }
        } else {
          yield value as FlatArray<CLEAN_VALUE, DEPTH>;
        }
      }
    });
  }
}
export function flat<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
>(depth: DEPTH = 0 as DEPTH): Stream.Transformer<NAME, SOURCE, Flat<SOURCE, CLEAN_VALUE, DEPTH, NAME>> {
  return (_, source, name) => new Flat(source, name, depth);
}

export namespace flat {
  export type Name = typeof NAME;
}

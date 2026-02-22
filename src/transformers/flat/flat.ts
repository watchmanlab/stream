import { Stream } from "../../streams/index.ts";

const NAME = "flat";

export class Flat<VALUE, DEPTH extends number = 0, NAME extends string = flat.Name> extends Stream<
  FlatArray<VALUE, DEPTH>,
  NAME
> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, depth = 0 as DEPTH) {
    super(name, async function* () {
      for await (const value of source) {
        if (Array.isArray(value)) {
          const values = value.flat(depth);
          for (let i = 0; i < values.length; i++) {
            yield values[i]!;
          }
        } else {
          yield value as FlatArray<VALUE, DEPTH>;
        }
      }
    });
  }
}
export function flat<VALUE, DEPTH extends number = 0, NAME extends string = flat.Name>(
  depth: DEPTH = 0 as DEPTH,
): Stream.Transformer<NAME, Stream<VALUE, any>, Flat<VALUE, DEPTH, NAME>> {
  return (_, source, name) => new Flat(source, name, depth);
}

export namespace flat {
  export type Name = typeof NAME;
}

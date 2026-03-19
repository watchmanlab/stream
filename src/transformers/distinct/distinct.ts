import { Stream } from "../../streams/index.ts";

const NAME = "distinct";

export class Distinct<VALUE, NAME extends string = distinct.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME) {
    super(name, async function* () {
      const seen = new Set<VALUE>();

      for await (const value of source) {
        if (seen.has(value)) continue;
        seen.add(value);
        yield value;
      }
    });
  }
}

export function distinct<VALUE, NAME extends string = distinct.Name>(): Stream.Transformer<
  NAME,
  Stream<VALUE, any>,
  Distinct<VALUE, NAME>
> {
  return (_, source, name) => new Distinct(source, name);
}

export namespace distinct {
  export type Name = typeof NAME;
}

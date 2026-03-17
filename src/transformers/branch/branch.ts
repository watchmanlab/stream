import { Stream } from "../../streams/index.ts";

const NAME = "branch";

export class Branch<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = branch.Name,
> extends Stream<Stream.ExtractValue<SOURCE>, NAME> {
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (!Stream.isSourceErr(value)) targets.forEach((target) => target.push(value));

        yield value;
      }
    });
  }
}

export function branch<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE = Stream.ExtractCleanValue<SOURCE>,
  NAME extends string = branch.Name,
>(
  ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
): Stream.Transformer<NAME, SOURCE, Branch<SOURCE, CLEAN_VALUE, NAME>> {
  return (_, source, name) => new Branch(source, name, ...targets);
}

export namespace branch {
  export type Name = typeof NAME;
}

import { Stream } from "../../streams/index.ts";

const NAME = "branch";

export class Branch<VALUE, NAME extends string = branch.Name> extends Stream<VALUE, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    ...targets: [Stream<Stream.ExtractValue<VALUE>, any>, ...Stream<Stream.ExtractValue<VALUE>, any>[]]
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (!Stream.isSourceErr(value)) targets.forEach((target) => target.push(value as Stream.ExtractValue<VALUE>));

        yield value;
      }
    });
  }
}

export function branch<VALUE, NAME extends string = branch.Name>(
  ...targets: [Stream<Stream.ExtractValue<VALUE>, any>, ...Stream<Stream.ExtractValue<VALUE>, any>[]]
): Stream.Transformer<NAME, Stream<VALUE, any>, Branch<VALUE, NAME>> {
  return (_, source, name) => new Branch(source, name, ...targets);
}

export namespace branch {
  export type Name = typeof NAME;
}

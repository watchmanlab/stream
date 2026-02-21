import { Stream } from "../../streams/index.ts";

const NAME = "branch";

export class Branch<VALUE, NAME extends string = branch.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, target: Stream<VALUE, any>) {
    super(name, async function* () {
      for await (const value of source) {
        target.push(value);
        yield value;
      }
    });
  }
}

export function branch<VALUE, NAME extends string = branch.Name>(
  target: Stream<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Branch<VALUE, NAME>> {
  return (_, source, name) => new Branch(source, name, target);
}

export namespace branch {
  export type Name = typeof NAME;
}

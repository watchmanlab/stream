import { Stream } from "../../streams";

const NAME = "distinctUntilChanged";

export class DistinctUntilChanged<VALUE, NAME extends string = distinctUntilChanged.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE>, name = NAME as NAME) {
    super(name, async function* () {
      let prev = Symbol() as VALUE;

      for await (const value of source) {
        if (prev === value) continue;
        prev = value;
        yield value;
      }
    });
  }
}

export function distinctUntilChanged<VALUE, NAME extends string = distinctUntilChanged.Name>(): Stream.Transformer<
  NAME,
  Stream<VALUE, any>,
  DistinctUntilChanged<VALUE, NAME>
> {
  return (_, source, name) => new DistinctUntilChanged(source, name);
}

export namespace distinctUntilChanged {
  export type Name = typeof NAME;
}

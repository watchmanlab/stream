import { Stream } from "../../streams";

const NAME = "distinctUntilChanged";

export class DistinctUntilChanged<
  SOURCE extends Stream<any, any>,
  NAME extends string = distinctUntilChanged.Name,
> extends Stream<Stream.ExtractValue<SOURCE>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME) {
    super(name, async function* () {
      let prev = Symbol();

      for await (const value of source) {
        if (Stream.isSentinel(value)) {
          yield value;
          continue;
        }
        if (prev === value) continue;
        prev = value;
        yield value;
      }
    });
  }
}

export function distinctUntilChanged<
  SOURCE extends Stream<any, any>,
  NAME extends string = distinctUntilChanged.Name,
>(): Stream.Transformer<NAME, SOURCE, DistinctUntilChanged<SOURCE, NAME>> {
  return (_, source, name) => new DistinctUntilChanged(source, name);
}

export namespace distinctUntilChanged {
  export type Name = typeof NAME;
}

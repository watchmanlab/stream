import { Stream } from "../../streams/index.ts";

const NAME = "derive";

export class Derive<SOURCE extends Stream<any, any>, NAME extends string = derive.Name> extends Stream<
  Stream.ExtractValue<SOURCE>,
  NAME
> {
  constructor(source: SOURCE, name = NAME as NAME) {
    super(name, source);
  }
}

export function derive<SOURCE extends Stream<any, any>, NAME extends string = derive.Name>(): Stream.Transformer<
  NAME,
  SOURCE,
  Derive<SOURCE, NAME>
> {
  return (_, source, name) => new Derive(source, name);
}

export namespace derive {
  export type Name = typeof NAME;
}

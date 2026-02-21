import { Stream } from "../../streams";
import { consumer } from "../consumer";

const NAME = "derive";

export class Derive<VALUE, NAME extends string = derive.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME) {
    super(name, source);
  }
}

export function derive<VALUE, NAME extends string = derive.Name>(): Stream.Transformer<
  NAME,
  Stream<VALUE, any>,
  Derive<VALUE, NAME>
> {
  return (_, source, name) => new Derive(source, name);
}

export namespace derive {
  export type Name = typeof NAME;
}

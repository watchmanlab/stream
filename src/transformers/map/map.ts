import { Stream } from "../../stream/stream";

const NAME = "mapped";
type Name = typeof NAME;

class Map<VALUE, MAPPED, NAME extends string = Name> extends Stream<MAPPED, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    private mapper: map.Mapper<VALUE, MAPPED>,
  ) {
    super(name, async function* () {
      for await (const value of source) {
        yield await mapper(value);
      }
    });
  }
}

export function map<VALUE, MAPPED, NAME extends string = Name>(
  mapper: map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, NAME>> {
  return (_, source, name) => new Map(source, name, mapper);
}

export namespace map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}

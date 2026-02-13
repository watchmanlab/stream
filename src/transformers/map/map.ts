import { Stream } from "../../stream";

export class Map<VALUE, MAPPED, NAME extends string = "mapped"> extends Stream<MAPPED, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    name = "mapped" as NAME,
    private mapper: map.Mapper<VALUE, MAPPED>,
  ) {
    super(async function* () {
      for await (const value of source) {
        yield await mapper(value);
      }
    }, name);
  }
}

export function map<VALUE, MAPPED, NAME extends string>(
  mapper: map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, NAME>> {
  return (source: Stream<VALUE, any>, name?: NAME) => new Map(source, name, mapper);
}

export namespace map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}

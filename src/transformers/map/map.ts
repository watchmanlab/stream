import { Stream } from "../../stream";

export class Map<VALUE, MAPPED> extends Stream<MAPPED, "mapped"> {
  constructor(
    source: Stream<VALUE, any>,
    private mapper: map.Mapper<VALUE, MAPPED>,
  ) {
    super(async function* () {
      for await (const value of source) {
        yield await mapper(value);
      }
    }, "mapped");
  }
}

export function map<VALUE, MAPPED>(
  mapper: map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<Stream<VALUE, any>, Map<VALUE, MAPPED>> {
  return (source) => new Map(source, mapper);
}

export namespace map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}

import { Stream } from "../../stream";

export class Map<VALUE, MAPPED, NAME extends string = Map.Name> extends Stream<MAPPED, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    private mapper: Map.Mapper<VALUE, MAPPED>,
    options?: Map.Options<NAME>,
  ) {
    const { name = Map.NAME as NAME } = options ?? {};

    super(name, async function* () {
      for await (const value of source) {
        yield await mapper(value);
      }
    });
  }
}

export function map<VALUE, MAPPED, NAME extends string = Map.Name>(
  mapper: Map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, NAME>> {
  return (source, name) => new Map(source, mapper, { name });
}

export namespace Map {
  export const NAME = "mapped";
  export type Name = typeof NAME;
  export type Options<NAME extends string> = {
    name?: NAME;
  };
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}

const stream = new Stream<number, "sof">("sof")
  .pipe(
    "toFixed",
    map((v) => v.toFixed()),
  )
  .pipe(
    map((v) => Number(v)),
    "toNumber",
  );

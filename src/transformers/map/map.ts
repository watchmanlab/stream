import { Stream } from "../../stream";

class Map<VALUE, MAPPED, NAME extends string = map.Name> extends Stream<MAPPED, NAME> {
  constructor(
    source: Stream<VALUE, any>,
    private mapper: map.Mapper<VALUE, MAPPED>,
    options?: map.Options<NAME>,
  ) {
    const { name = map.NAME as NAME } = options ?? {};

    super(name, async function* () {
      for await (const value of source) {
        yield await mapper(value);
      }
    });
  }
}

export function map<VALUE, MAPPED, NAME extends string = map.Name>(
  mapper: map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Map<VALUE, MAPPED, NAME>> {
  return (_, source, name) => new Map(source, mapper, { name });
}

export namespace map {
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

let f = map((v) => v)();

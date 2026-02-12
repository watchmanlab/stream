import { Stream } from "../../stream";

export function map<VALUE, MAPPED>(
  mapper: map.Mapper<VALUE, MAPPED>,
): Stream.Transformer<Stream<VALUE, any>, Stream<MAPPED, "mapped">> {
  return (source) =>
    new Stream(async function* () {
      for await (const value of source) {
        return await mapper(value);
      }
    }, "mapped");
}

export namespace map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED | Promise<MAPPED>;
}

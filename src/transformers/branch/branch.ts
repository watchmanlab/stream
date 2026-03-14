import { Stream } from "../../streams/index.ts";
import { filter } from "../filter/filter.ts";
import { map } from "../map";

const NAME = "branch";

export class Branch<SOURCE extends Stream<any, any>, NAME extends string = branch.Name> extends Stream<
  Stream.ExtractValueFromSource<SOURCE>,
  NAME
> {
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    ...targets: [
      Stream<Stream.ExtractCleanValueFromSource<SOURCE>, any>,
      ...Stream<Stream.ExtractCleanValueFromSource<SOURCE>, any>[],
    ]
  ) {
    super(name, async function* () {
      for await (const value of source) {
        if (!Stream.isSourceErr(value))
          targets.forEach((target) => target.push(value as Stream.ExtractCleanValueFromSource<SOURCE>));

        yield value;
      }
    });
  }
}

export function branch<SOURCE extends Stream<any, any>, NAME extends string = branch.Name>(
  ...targets: [
    Stream<Stream.ExtractCleanValueFromSource<SOURCE>, any>,
    ...Stream<Stream.ExtractCleanValueFromSource<SOURCE>, any>[],
  ]
): Stream.Transformer<NAME, SOURCE, Branch<SOURCE, NAME>> {
  return (_, source, name) => new Branch(source, name, ...targets);
}

export namespace branch {
  export type Name = typeof NAME;
}

const s1 = new Stream([1, 2]);

const s2 = new Stream([3, 4])
  .pipe(
    map((v) => {
      if (v > 3) return Stream.err("kechmahaja" as const);
      return v;
    }),
  )
  .pipe(branch(s1))
  .pipe(filter((v) => v > 4));

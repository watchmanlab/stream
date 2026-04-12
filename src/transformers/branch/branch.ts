import { Stream } from "../../streams/index.ts";

const NAME = "branch";

class Branch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = branch.Name,
> extends Stream<Stream.ExtractValue<INPUT_STREAM>, NAME> {
  constructor(
    name: NAME,
    inputStream: INPUT_STREAM,
    ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
  ) {
    super(name, async function* () {
      for await (const value of inputStream) {
        if (!Stream.isSourceErr(value)) targets.forEach((target) => target.push(value));

        yield value;
      }
    });
  }
}
export function branch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = branch.Name,
>(
  ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Branch<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function branch<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
>(
  name: NAME,
  ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Branch<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>>;
export function branch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  NAME extends string = branch.Name,
>(
  nameOrTarget: NAME | Stream<CLEAN_VALUE, any>,
  ...targets: [Stream<CLEAN_VALUE, any>, ...Stream<CLEAN_VALUE, any>[]]
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Branch<INPUT_STREAM, CLEAN_VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrTarget === "string"
        ? new Branch(nameOrTarget, inputStream, ...targets)
        : new Branch(NAME as NAME, inputStream, nameOrTarget, ...targets),
      inputStream,
    );
}

export namespace branch {
  export type Name = typeof NAME;
}

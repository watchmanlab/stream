import { Stream } from "../../../streams/index.ts";

const NAME = "audit";

class Audit<INPUT_STREAM extends Stream.AnyStream, NAME extends string = audit.Name> extends Stream<
  Stream.ExtractValue<INPUT_STREAM>,
  NAME
> {
  constructor(name: NAME, inputStream: INPUT_STREAM, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let canEmit = true;

      try {
        for await (const value of inputStream) {
          if (Stream.isSentinel(value)) {
            yield value;
            continue;
          }

          if (!canEmit) continue;
          canEmit = false;
          clearTimeout(timer);
          timer = setTimeout(() => (canEmit = true), ms);
          yield value;
        }
      } finally {
        clearTimeout(timer);
      }
    });
  }
}

export function audit<INPUT_STREAM extends Stream.AnyStream, NAME extends string = audit.Name>(
  ms: number,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Audit<INPUT_STREAM, NAME>, INPUT_STREAM>>;
export function audit<NAME extends string, INPUT_STREAM extends Stream.AnyStream>(
  name: NAME,
  ms: number,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Audit<INPUT_STREAM, NAME>, INPUT_STREAM>>;
export function audit<INPUT_STREAM extends Stream.AnyStream, NAME extends string = audit.Name>(
  nameOrMs: NAME | number,
  ms?: number,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Audit<INPUT_STREAM, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrMs === "string"
        ? new Audit(nameOrMs, inputStream, ms!)
        : new Audit(NAME as NAME, inputStream, nameOrMs),
      inputStream,
    );
}

export namespace audit {
  export type Name = typeof NAME;
  export type Options = {
    type: "time" | "count";
  };
}

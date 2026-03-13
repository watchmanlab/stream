import { Stream } from "../../streams/index.ts";

const NAME = "batch";

type FixedArray<VALUE, SIZE extends number, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export class Batch<VALUE, SIZE extends number, NAME extends string = batch.Name> extends Stream<
  FixedArray<Stream.ExtractValue<VALUE>, SIZE> | Stream.ExtractError<VALUE>,
  NAME
> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, size: SIZE) {
    const buf = new Array<Stream.ExtractValue<VALUE>>();
    super(name, async function* () {
      try {
        for await (const value of source) {
          if (Stream.isSourceErr(value)) {
            yield value as Stream.ExtractError<VALUE>;
            continue;
          }
          const safeValue = value as Stream.ExtractValue<VALUE>;

          buf.push(safeValue);
          if (buf.length >= size) {
            yield [...buf] as FixedArray<VALUE, SIZE>;
            buf.length = 0;
          }
        }
      } finally {
        buf.length = 0;
      }
    });
  }
}
export function batch<VALUE, SIZE extends number, NAME extends string = batch.Name>(
  size: SIZE,
): Stream.Transformer<NAME, Stream<VALUE, any>, Batch<VALUE, SIZE, NAME>> {
  return (_, source, name) => new Batch(source, name, size);
}

export namespace batch {
  export type Name = typeof NAME;
}

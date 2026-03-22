import { Stream } from "../../streams/index.ts";
import { FixedArray } from "../../types/index.ts";

const NAME = "batch";

export class Batch<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  SIZE extends number = 2,
  NAME extends string = batch.Name,
> extends Stream<FixedArray<CLEAN_VALUE, SIZE> | Stream.ExtractSentinel<SOURCE>, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, size: SIZE) {
    const buf = new Array<CLEAN_VALUE>();
    super(name, async function* () {
      try {
        for await (const value of source) {
          if (Stream.isSentinel(value)) {
            yield value as never;
            continue;
          }
          const cleanValue = value as CLEAN_VALUE;

          buf.push(cleanValue);
          if (buf.length >= size) {
            yield [...buf] as FixedArray<CLEAN_VALUE, SIZE>;
            buf.length = 0;
          }
        }
      } finally {
        buf.length = 0;
      }
    });
  }
}
export function batch<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  SIZE extends number = 2,
  NAME extends string = batch.Name,
>(size: SIZE): Stream.Transforme<NAME, SOURCE, Batch<SOURCE, CLEAN_VALUE, SIZE, NAME>> {
  return (_, source, name) => new Batch(source, name, size);
}

export namespace batch {
  export type Name = typeof NAME;
}

const stream = new Stream([1, 2, 3])
  .pipe(batch(2))
  .next()
  .then((r) => r.value);

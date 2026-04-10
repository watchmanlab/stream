import { Stream } from "../../streams/index.ts";
import { FixedArray } from "../../types/index.ts";
import { each } from "../each/each.ts";
import { pump } from "../pump/pump.ts";

const NAME = "batch";

export class Batch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SIZE extends number = 2,
  NAME extends string = batch.Name,
> extends Stream<FixedArray<CLEAN_VALUE, SIZE> | Stream.ExtractSentinel<INPUT_STREAM>, NAME> {
  private _buffer = new Array<CLEAN_VALUE>();
  constructor(name: NAME, inputStream: INPUT_STREAM, size: SIZE) {
    super(name, async function* () {
      try {
        for await (const value of inputStream) {
          if (Stream.isSentinel(value)) {
            yield value as never;
            continue;
          }

          self._buffer.push(value);
          if (self._buffer.length >= size) {
            yield [...self._buffer] as FixedArray<CLEAN_VALUE, SIZE>;
            self._buffer.length = 0;
          }
        }
      } finally {
        self._buffer.length = 0;
      }
    });
    const self = this;
  }

  get buffer() {
    return this._buffer;
  }
}
export function batch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SIZE extends number = 2,
  NAME extends string = batch.Name,
>(
  size: SIZE,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Batch<INPUT_STREAM, CLEAN_VALUE, SIZE, NAME>, INPUT_STREAM>>;
export function batch<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SIZE extends number = 2,
>(
  name: NAME,
  size: SIZE,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Batch<INPUT_STREAM, CLEAN_VALUE, SIZE, NAME>, INPUT_STREAM>>;
export function batch<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE = Stream.ExtractCleanValue<INPUT_STREAM>,
  SIZE extends number = 2,
  NAME extends string = batch.Name,
>(
  nameOrSize: NAME | SIZE,
  size?: SIZE,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Batch<INPUT_STREAM, CLEAN_VALUE, SIZE, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrSize === "string"
        ? new Batch(nameOrSize, inputStream, size!)
        : new Batch(NAME as NAME, inputStream, nameOrSize),
      inputStream,
    );
}

export namespace batch {
  export type Name = typeof NAME;
}

const stream = new Stream([1, 2, 3, 4, 5, 6, 7, 8, 9])
  .pipe(batch(2))
  .pipe(each(console.log))
  .pipe(pump())
  .pipe(each((v) => {}));

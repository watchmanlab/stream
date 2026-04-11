import { Stream } from "../../streams/index.ts";

const NAME = "debounce";

export class Debounce<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = debounce.Name,
> extends Stream<VALUE, NAME> {
  constructor(name: NAME, inputStream: INPUT_STREAM, ms: number) {
    super(name, async function* () {
      let timer: any = null;
      let aborted = false;

      let resolve: (value: VALUE) => void = () => {};
      const generator = inputStream[Symbol.asyncIterator]();

      (async () => {
        for await (const value of generator) {
          if (Stream.isSentinel(value)) {
            resolve(value as never);
            continue;
          }
          clearTimeout(timer);
          if (aborted) {
            resolve(value);
            break;
          }
          timer = setTimeout(() => resolve(value), ms);
        }
      })();

      try {
        while (true) {
          yield await new Promise<VALUE>((r) => (resolve = r));
        }
      } finally {
        clearTimeout(timer);
        aborted = true;
        resolve(Stream.TERMINATE as never);
        await generator.return();
      }
    });
  }
}
export function debounce<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = debounce.Name,
>(ms: number): Stream.Transform<INPUT_STREAM, Stream.Traversable<Debounce<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>;
export function debounce<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
>(
  name: NAME,
  ms: number,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Debounce<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>;
export function debounce<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = debounce.Name,
>(
  nameOrMs: NAME | number,
  ms?: number,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Debounce<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrMs === "string"
        ? new Debounce(nameOrMs, inputStream, ms!)
        : new Debounce(NAME as NAME, inputStream, nameOrMs),
      inputStream,
    );
}

export namespace debounce {
  export type Name = typeof NAME;
}

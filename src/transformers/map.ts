import { Stream, Transformer, Source, Queue } from "../stream/index.ts";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR extends { error: unknown; reason: unknown } = { error: unknown; reason: unknown },
  NAME extends string = map.Name,
> extends Transformer<INPUT_STREAM, MAPPED, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name, inputStream, () => {
      const consumer = inputStream.getConsumer();

      return {
        next: async () => {
          const iteratorResult = await consumer.next();
          if (iteratorResult.done) return { value: Queue.EMPTY, done: true };
          const value = mapper(iteratorResult.value);
          return { value: value instanceof Promise ? await value : value };
        },
        return: async () => {
          await consumer.return();
          return { value: Queue.EMPTY as never, done: true };
        },
      };
    });
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR extends { error: unknown; reason: unknown } = { error: unknown; reason: unknown },
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => {
    return new Map(name, inputStream, mapper);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, ERROR extends { error: unknown; reason: unknown }> = (
    value: VALUE,
  ) => MAPPED | Source.Error<ERROR> | Promise<MAPPED | Source.Error<ERROR>>;
}

function bench() {
  const MAX = 1_000_000;
  const start = performance.now();

  const stream = new Stream<number>();
  const mapped = stream
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v))
    .pipe(map((v) => v));

  (async () => {
    for await (const value of mapped) {
      if (value === MAX) console.log("iter ", Math.round(performance.now() - start));
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

bench();

// (async () => {
// const progress = stream.traversal.root.push();
// if (!map1.source) return;
// for await (const error of map1.source.error) {
//   console.log(error);
//   error.source;
// }
// })();

import { Stream, Transformer, Source } from "../stream/index.ts";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
> extends Transformer<INPUT_STREAM, MAPPED, ERROR, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name, inputStream, () => {
      const consumer = inputStream.getConsumer();

      return {
        next: async () => {
          const iteratorResult = await consumer.next();
          if (iteratorResult.done) return iteratorResult;
          const value = mapper(iteratorResult.value);
          return { value: value instanceof Promise ? await value : value };
        },
        return: () => {
          return consumer.return();
        },
      };
    });
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
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
  export type Mapper<VALUE, MAPPED, ERROR> = (
    value: VALUE,
  ) => MAPPED | Source.Error<ERROR> | Promise<MAPPED | Source.Error<ERROR>>;
}

function bench() {
  const MAX = 1_000_000;
  const start = performance.now();

  const stream = new Stream<number>();
  const mapped = stream.pipe(
    map((v) => {
      if (v === 4) return new Source.Error("kechmahaja " + v);
      return v.toFixed();
    }),
  );
  mapped.source?.error.next().then((res) => console.log(res.value));
  (async () => {
    for await (const value of mapped) {
      if (value === MAX.toFixed()) console.log("iter ", Math.round(performance.now() - start));
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

bench();

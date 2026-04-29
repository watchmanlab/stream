import { Stream } from "./stream";

const NAME = "map";
class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = unknown,
  NAME extends string = map.Name,
> extends Stream.Transformer<INPUT_STREAM, MAPPED, { error: ERROR; reason: VALUE }, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name, inputStream, async function* () {
      for await (const value of inputStream) {
        try {
          const maybePromise = mapper(value);
          const result = maybePromise instanceof Promise ? await maybePromise : maybePromise;
          if (result instanceof Stream.Error) {
            yield new Stream.Error({ error: result.data, reason: value });
          } else {
            yield result;
          }
        } catch (error: any) {
          if (error instanceof Stream.Error) {
            yield new Stream.Error({ error: error.data, reason: value });
          } else {
            yield new Stream.Error({ error, reason: value });
          }
        }
      }
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
  ) => MAPPED | Stream.Error<ERROR> | Promise<MAPPED | Stream.Error<ERROR>>;
}
//

const stream = new Stream([1, 2, 3])
  .pipe(
    "map1",
    map((v) => {
      if (v !== 2) {
        return v.toFixed();
      } else {
        return new Stream.Error("kechma");
      }
    }),
  )
  .pipe(
    "map2",
    map((v) => v),
  )
  .pipe(
    "map3",
    map((v) => v),
  )
  .pipe(
    "map4",
    map((v) => v),
  )
  .pipe(
    "map5",
    map((v) => v),
  );
// const v = stream.traversal.map4.map3.map2.map1.source?.error.name;
// console.log(v);

const map1 = stream.traversal.map4.map3.map2.map1.consumers;

(async () => {
  // if (!map1.source) return;
  // for await (const error of map1.source.error) {
  //   console.log(error);
  //   error.source;
  // }
})();

(async () => {
  for await (const value of stream) {
    // console.log(value);
  }
})();

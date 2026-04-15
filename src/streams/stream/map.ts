import { Stream } from "./stream9";

const NAME = "map";

class Map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<INPUT_STREAM>
  | Stream.MaybeSourceErr<
      CLEAN_VALUE,
      ERROR,
      Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
    >,
  NAME
> {
  constructor(name: NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>) {
    super(name);

    const signal = new Stream();
    this.hooks = {
      afterFirstListenerAdded: () => {
        inputStream.listen(async (value) => {
          if (value === Stream.TERMINATE) {
            this.terminate();
            return;
          }
          const result = mapper(value);

          if (Stream.isErr(result)) {
            this.push(Stream.sourceErr({ value: value, error: result.value, source: this }) as never);
          } else {
            this.push(result);
          }
        }, signal);
      },
      afterLastListenerRemoved: signal.terminate.bind(signal),
      afterTerminate: signal?.terminate.bind(signal),
    };
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
>;
export function map<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
>(
  name: NAME,
  mapper: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  CLEAN_VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  nameOrMapper: NAME | map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  mapper?: map.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
): Stream.Transform<
  INPUT_STREAM,
  Stream.Traversable<Map<INPUT_STREAM, CLEAN_VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrMapper === "string"
        ? new Map(nameOrMapper, inputStream, mapper!)
        : new Map(NAME as NAME, inputStream, nameOrMapper),
      inputStream,
    );
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<CLEAN_VALUE, MAPPED, ERROR> = (value: CLEAN_VALUE) => MAPPED | Stream.Err<ERROR>;
}

const stream = new Stream<number>();
const mapped = stream
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(map((v) => v));

const now = performance.now();
const MAX = 1_000_000;
mapped.listen((v) => {
  if (v === MAX) console.log("hot", performance.now() - now);
});

// (async () => {
//   for await (const v of mapped) {
//     if (v === MAX) console.log("cold", performance.now() - now);
//   }
// })();
for (let i = 1; i <= MAX; i++) {
  stream.push(i);
}

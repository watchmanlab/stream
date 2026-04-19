import { Stream } from "./stream";

const NAME = "map";

class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
> extends Stream<MAPPED, NAME> {
  private _error?: Stream<{ value: VALUE; error: ERROR }, `${NAME}Error`>;

  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, ERROR>) {
    super(name);

    const signal = new Stream();

    this.afterFirstListenerAdded.listen(() => {
      inputStream.listen((value) => {
        try {
          const result = mapper(value);

          if (Stream.isErr(result)) {
            result.sourceName = this.name;
            if (!this._error?.listenersCount) throw result;
            this._error?.push({ value, error: result.value });
          } else {
            this.push(result);
          }
        } catch (error: any) {
          if (!this._error?.listenersCount) throw error;
          this._error?.push({ value, error });
        }
      }, signal);
    });
    this.afterLastListenerRemoved.listen(() => {
      signal.push();
    });
    this.afterTerminate.listen(() => {
      signal.push();
    });
  }

  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, ERROR>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>> {
  return (inputStream, name) => Stream.transformer(new Map(name, inputStream, mapper), inputStream);
}

export namespace map {
  export type Name = typeof NAME;

  export type Mapper<VALUE, MAPPED, ERROR> = (value: VALUE) => MAPPED | Stream.Err<ERROR>;
}

const stream = new Stream<number>();
const mapped = stream
  .pipe(
    "map1",
    map((v) => v),
  )
  .pipe(
    "map2",
    map((v) => v),
  )

  .pipe(
    map((v) => {
      // if (v === 4) return Stream.err("kechmahaja" as const);
      return v;
    }),
  );

const now = performance.now();
const MAX = 1_000_000;
mapped.listen((v) => {
  if (v === MAX) console.log("hot", performance.now() - now);
});
mapped.error.listen((err) => {
  console.log(err);
});

// (async () => {
//   for await (const v of mapped) {
//     if (v === MAX) console.log("cold", performance.now() - now);
//   }
// })();
for (let i = 1; i <= MAX; i++) {
  stream.push(i);
}

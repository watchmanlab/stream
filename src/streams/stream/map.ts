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
  private signal = new Stream();
  constructor(
    name: NAME,
    private inputStream: INPUT_STREAM,
    private mapper: map.Mapper<
      VALUE,
      MAPPED,
      ERROR,
      Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
    >,
  ) {
    super(name);

    this.hooks = {
      afterFirstListenerAdded: () => this.transform(),
      afterLastListenerRemoved: this.signal.terminate.bind(this.signal),
      afterTerminate: this.signal?.terminate.bind(this.signal),
    };
  }

  private transform() {
    const self = this;
    self.inputStream.listen(function (value) {
      try {
        const result = self.mapper(value, Stream.traversable(self, self.inputStream));

        if (Stream.isErr(result)) {
          if (!self._error) throw result.value;
          self._error.push({ value, error: result.value });
        } else {
          self.push(result);
        }
      } catch (error: any) {
        if (!self._error) throw error;
        self._error.push({ value, error });
      }
    }, self.signal);
  }
  get error() {
    if (!this._error) this._error = new Stream(`${this.name}Error`);
    return this._error;
  }
}

export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<
    VALUE,
    MAPPED,
    ERROR,
    Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>>;
export function map<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
>(
  name: NAME,
  mapper: map.Mapper<
    VALUE,
    MAPPED,
    ERROR,
    Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractCleanValue<INPUT_STREAM> = Stream.ExtractCleanValue<INPUT_STREAM>,
  MAPPED = VALUE,
  ERROR = never,
  NAME extends string = map.Name,
>(
  nameOrMapper:
    | NAME
    | map.Mapper<VALUE, MAPPED, ERROR, Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>>,
  mapper?: map.Mapper<
    VALUE,
    MAPPED,
    ERROR,
    Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>
  >,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Map<INPUT_STREAM, VALUE, MAPPED, ERROR, NAME>, INPUT_STREAM>> {
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

  export type Mapper<VALUE, MAPPED, ERROR, THIS extends Stream.AnyTraversable> = (
    value: VALUE,
    self: THIS,
  ) => MAPPED | Stream.Err<ERROR>;
}

const stream = new Stream<number>();
const mapped = stream
  .pipe(map((v) => v))
  .pipe(map((v) => v))
  .pipe(
    map("custom", (v, self) => {
      self.name;
      return v;
    }),
  )
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

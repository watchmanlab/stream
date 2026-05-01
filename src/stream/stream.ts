import { Consumers } from "./consumers";
import { Source } from "./source";

const NAME = "root";

export class Stream<VALUE, ERROR, NAME extends string = Stream.Name> implements AsyncIterable<VALUE>, Disposable {
  private _consumers: Consumers<VALUE, NAME>;
  private _source?: Source<VALUE, ERROR, NAME>;
  readonly name: NAME;
  constructor();
  constructor(name: NAME);
  constructor(sourceData: Source.SourceData<VALUE, ERROR>);
  constructor(name: NAME, sourceData: Source.SourceData<VALUE, ERROR>);
  constructor(
    nameOrSourceData?: NAME | Source.SourceData<VALUE, ERROR>,
    _sourceData?: Source.SourceData<VALUE, ERROR>,
  ) {
    let sourceData: Source.SourceData<VALUE, ERROR> | undefined;
    if (typeof nameOrSourceData === "string") {
      this.name = nameOrSourceData;
      sourceData = _sourceData;
    } else {
      this.name = NAME as NAME;
      sourceData = nameOrSourceData;
    }

    if (sourceData)
      this._source = new Source(
        this.name,
        sourceData,
        (value) => this.push(value),
        () => (this._source = undefined),
      );

    this._consumers = new Consumers(this.name, { source: this._source });
  }
  get consumers() {
    return this._consumers;
  }
  get source() {
    return this._source;
  }

  [Symbol.asyncIterator]() {
    return this._consumers.getConsumer(`${this._consumers.count}`)[Symbol.asyncIterator]();
  }
  [Symbol.dispose]() {
    this.terminate();
  }
  push<const T extends VALUE>(value: T) {
    return this._consumers.push(value);
  }
  next() {
    return this[Symbol.asyncIterator]().next();
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Stream.Transformer<this, any, any, OUTPUT_NAME>>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  terminate(terminateSource = true) {
    this._consumers.terminate();
    if (terminateSource) this._source?.terminate();
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any, any>;
  export type AnyTransformer = Transformer<AnyStream, any, any, any>;

  export type ExtractValue<T extends AnyStream | AnyTransformer> =
    T extends Stream<infer VALUE, any, any> ? VALUE : T extends Transformer<any, infer VALUE, any, any> ? VALUE : never;

  export type ExtractError<T extends Source.AnyError | AnyStream | AnyTransformer> =
    T extends Source.Error<infer ERROR>
      ? ERROR
      : T extends Stream<any, infer ERROR, any>
        ? ERROR
        : T extends Transformer<any, any, infer ERROR, any>
          ? ERROR
          : never;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;

  export type ExtractInputStream<T extends AnyStream> =
    T extends Transformer<infer INPUT_STREAM, any, any, any> ? INPUT_STREAM : never;
  export type Traversable<T extends AnyStream> =
    ExtractInputStream<T> extends never
      ? T
      : Omit<T, "traversal"> &
          Record<ExtractInputStream<T>["name"] | (`$${string}` & {}), Traversable<ExtractInputStream<T>>>;
  export abstract class Transformer<
    INPUT_STREAM extends Stream.AnyStream,
    VALUE,
    ERROR extends { error: unknown; reason: ExtractValue<INPUT_STREAM> },
    NAME extends string,
  > extends Stream<VALUE, ERROR, NAME> {
    constructor(
      name: NAME,
      protected readonly inputStream: INPUT_STREAM,
      fn?: () => AsyncGenerator<VALUE | Source.Error<ERROR>>,
    ) {
      super(name, fn!);

      return new Proxy(this, {
        get(target, p, receiver) {
          if (p in target) return Reflect.get(target, p, receiver);
          return inputStream;
        },
      });
    }
    get traversal(): Record<INPUT_STREAM["name"] | (`$${string}` & {}), Traversable<INPUT_STREAM>> {
      const self = this;
      return new Proxy(
        {},
        {
          get() {
            return self.inputStream;
          },
        },
      ) as never;
    }
  }

  export class SourceError<ERROR, SOURCE extends AnyStream> {
    constructor(
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {}
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }

  export const TERMINATED = Symbol.for("$TERMINATED#");
  export type Terminated = typeof TERMINATED;
}

function simpleTest() {
  const stream = new Stream<number, never>(async function* () {
    // await new Promise((r) => setTimeout(r, 10));
    // yield 1 as number;
    // await new Promise((r) => setTimeout(r, 10));
    // yield 2;
    // await new Promise((r) => setTimeout(r, 10));
    // yield 3;
  });

  (async () => {
    for await (const value of stream) {
      console.log("c1", value);
      if (value == 2) break;
    }
  })();
  (async () => {
    for await (const value of stream) {
      console.log("c2", value);
      if (value == 2) break;
    }
  })();

  stream.push(44);
  stream.push(55);
}
function newStreamBench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number, never>();
  (async () => {
    for await (const value of stream) {
      let result = value + 10;
      if (result === 40010) {
        result = 444;
      } else {
        result = 555;
      }

      if (value === MAX) {
        console.log("bench", Math.round(performance.now() - now));
        return;
      }
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

// simpleTest();
// newStreamBench();

function loadbalancing() {
  const consumer = new Stream([1, 2, 3]).consumers.getConsumer("dd");
  setTimeout(() => {
    consumer.terminate();
  }, 100);
  (async () => {
    for await (const value of consumer) {
      console.log("c1", value);
    }
    console.log("done");
  })();
  (async () => {
    for await (const value of consumer) {
      console.log("c2", value);
    }
    console.log("done");
  })();
}
loadbalancing();

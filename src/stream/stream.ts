import { Dispatcher } from "./dispatcher";
import { Source } from "./source";
import { Transformer } from "./transformer";

const NAME = "root";

export class Stream<VALUE, ERROR, NAME extends string = Stream.Name>
  implements AsyncIterable<VALUE>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _dispatcher: Dispatcher<VALUE, NAME>;
  private _source?: Source<VALUE, ERROR, NAME>;

  constructor(name: NAME, sourceData?: Source.SourceData<VALUE, ERROR>);
  constructor(sourceData?: Source.SourceData<VALUE, ERROR>);
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

    this._dispatcher = new Dispatcher(this.name, { source: this._source });
  }

  [Symbol.asyncIterator]() {
    return this._dispatcher.getConsumer();
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  push<const T extends VALUE>(value: T) {
    return this._dispatcher.dispatch(value);
  }
  async next() {
    const consumer = this._dispatcher.getConsumer();
    const result = await consumer.next();
    await consumer.dispose();
    return result;
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, any, OUTPUT_NAME>>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, any, OUTPUT_NAME>>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, any, OUTPUT_NAME>>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  async dispose() {
    await Promise.all([this._dispatcher.dispose(), this._source?.dispose()]);
  }
  get dispatcher() {
    return this._dispatcher;
  }
  get source() {
    return this._source;
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any, any>;
  export type ExtractValue<T extends AnyStream> = T extends Stream<infer VALUE, any, any> ? VALUE : never;
  export type ExtractError<T extends AnyStream> = T extends Stream<any, infer ERROR, any> ? ERROR : never;
  export type ExtractName<T extends AnyStream> = T["name"];

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;

  export class SourceError<ERROR, SOURCE extends AnyStream> {
    constructor(
      public readonly error: ERROR,
      public readonly source: SOURCE,
    ) {}
    get sourceName(): SOURCE["name"] {
      return this.source.name;
    }
  }
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
function bench() {
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
function consumerTest() {
  const stream1 = new Stream("clicks", new String("hi"));
  const consumer1 = stream1.dispatcher.getConsumer();
  const consumer2 = stream1.dispatcher.getConsumer();

  (async () => {
    for await (const value of consumer1) {
      console.log(consumer1.buffer.name, value);
    }
    console.log(consumer1.name, " done");
  })();
  (async () => {
    for await (const value of consumer2) {
      console.log(consumer2.buffer.name, value);
    }
    console.log(consumer2.name, " done");
  })();

  // stream1.push(1);
  // stream1.push(2);
  // stream.push(3)
}

// simpleTest();
bench();
consumerTest();

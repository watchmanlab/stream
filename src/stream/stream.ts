import { Consumer } from "./consumer";
import { Queue } from "./queue";
import { Source } from "./source";
import { Transformer } from "./transformer";

const NAME = "root";

export class Stream<VALUE, ERROR, NAME extends string = Stream.Name>
  implements AsyncIterable<VALUE>, Iterable<Consumer<VALUE, NAME>>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _source?: Source<VALUE, ERROR, NAME>;
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerAttached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerAttached`>;
  private _consumerDetached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerDetached`>;
  private _cleared?: Stream<undefined, never, `${NAME}Cleared`>;
  private _disposed?: Stream<undefined, never, `${NAME}Disposed`>;
  constructor(name: NAME, sourceData?: Source.SourceData<VALUE, ERROR>);
  constructor(sourceData?: Source.SourceData<VALUE, ERROR>);
  constructor(nameOrSourceData?: NAME | Source.SourceData<VALUE, ERROR>, sourceData?: Source.SourceData<VALUE, ERROR>) {
    if (typeof nameOrSourceData === "string") {
      this.name = nameOrSourceData;
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
  }
  [Symbol.asyncIterator]() {
    return this.getConsumer();
  }
  [Symbol.iterator]() {
    return this._consumers.values();
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  push<const T extends VALUE>(value: T): Consumer.PushProgress<T, string>[] {
    const consumers = this._consumers;

    const progresses: Consumer.PushProgress<T, string>[] = [];

    for (const consumer of consumers.values()) {
      progresses.push(consumer.push(value) as never);
    }
    return progresses;
  }
  getConsumer(options?: {
    bufferOptions?: Queue.Options;
    pendingsOptions?: Queue.Options;
  }): Consumer<VALUE, `${NAME}Consumer${string}`> {
    let name: `${NAME}Consumer${string}`;

    while (true) {
      name = `${this.name}Consumer${globalThis.crypto.getRandomValues(new Uint32Array(1))[0]}`;
      if (!this._consumers.has(name)) break;
    }

    const consumer = new Consumer(name, {
      source: this._source,
      onTerminate: () => {
        this._consumers.delete(name);
        this._consumerDetached?.push(consumer);
      },
      ...options,
    });

    this._consumers.set(name, consumer);
    this._consumerAttached?.push(consumer);
    return consumer;
  }
  async next() {
    const consumer = this.getConsumer();
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
  async clear() {
    const promises = [];
    for (const consumer of this) {
      promises.push(consumer.return());
    }

    await Promise.all(promises);
    this._cleared?.push(undefined);
  }
  async dispose() {
    await Promise.all([
      this.clear(),
      this._consumerAttached?.dispose(),
      this._consumerDetached?.dispose(),
      this._cleared?.dispose(),
    ]);
    this._consumerAttached = this._consumerDetached = this._cleared = undefined;

    this._disposed?.push(undefined);
    await this._disposed?.dispose();
    this._disposed = undefined;
  }

  get source() {
    return this._source;
  }
  get consumersCount() {
    return this._consumers.size;
  }
  get consumerAttached() {
    if (!this._consumerAttached) {
      this._consumerAttached = new Stream(`${this.name}ConsumerAttached`);
    }
    return this._consumerAttached;
  }
  get consumerDetached() {
    if (!this._consumerDetached) {
      this._consumerDetached = new Stream(`${this.name}ConsumerDetached`);
    }
    return this._consumerDetached;
  }
  get cleared() {
    if (!this._cleared) {
      this._cleared = new Stream(`${this.name}Cleared`);
    }
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) {
      this._disposed = new Stream(`${this.name}Disposed`);
    }
    return this._disposed;
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any, any>;
  export type ExtractValue<T> =
    T extends Stream<infer VALUE, any, any>
      ? VALUE
      : T extends Transformer<any, infer VALUE, any, any>
        ? VALUE
        : T extends Source<infer VALUE, any, any>
          ? VALUE
          : T extends Source.SourceData<infer VALUE, any>
            ? VALUE
            : never;
  export type ExtractName<T extends AnyStream | Transformer.AnyTransformer | Source.AnySource> = T["name"];
  export type ExtractError<
    T extends AnyStream | Transformer.AnyTransformer | Source.AnyError | Source.AnySource | Source.AnySourceData,
  > =
    T extends Stream<any, infer ERROR, any>
      ? ERROR
      : T extends Transformer<any, any, infer ERROR, any>
        ? ERROR
        : T extends Source.AnyError
          ? T["data"]
          : T extends Source<any, infer ERROR, any>
            ? ERROR
            : T extends Source.SourceData<any, infer ERROR>
              ? ERROR
              : never;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;
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
  const stream1 = new Stream("clicks", () => {
    let i = 0;
    return {
      next: async () => {
        return { value: i++, done: i > 2 };
      },
    };
  });

  const consumer1 = stream1.getConsumer();
  const consumer2 = stream1.getConsumer();

  (async () => {
    for await (const value of consumer1) {
      await new Promise((r) => setTimeout(r, 300));

      console.log(consumer1.name, value);
    }
    console.log(consumer1.name, " done");
  })();
  (async () => {
    for await (const value of consumer2) {
      console.log(consumer2.name, value);
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

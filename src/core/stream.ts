import { Consumer } from "./consumer.ts";
import { Queue } from "./queue.ts";
import { Source } from "./source.ts";
import { Transformer } from "./transformer.ts";

const NAME = "root";

export class Stream<VALUE, ERROR = unknown, NAME extends string = Stream.Name>
  implements
    AsyncIterable<Stream.Batch<VALUE>>,
    Iterable<Consumer<VALUE, Stream.ConsumerName<NAME>>>,
    AsyncDisposable,
    Disposable
{
  readonly name: NAME;
  private _source?: Source<VALUE, ERROR, NAME>;
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerAttached?: Stream<Consumer<VALUE, string>, never, `${NAME}ConsumerAttached`>;
  private _consumerDetached?: Stream<Consumer<VALUE, string>, never, `${NAME}ConsumerDetached`>;
  private _cleared?: Stream<void, never, `${NAME}Cleared`>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;
  constructor(name: NAME, sourceData?: Source.SourceData<VALUE>);
  constructor(sourceData?: Source.SourceData<VALUE>);
  constructor(nameOrSourceData?: NAME | Source.SourceData<VALUE>, sourceData?: Source.SourceData<VALUE>) {
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
        (values) => this.batch(values),
        () => (this._source = undefined),
      );
  }
  [Symbol.asyncIterator](): Consumer<VALUE, Stream.ConsumerName<NAME>> {
    return this.getConsumer();
  }
  [Symbol.iterator](): MapIterator<Consumer<VALUE, Stream.ConsumerName<NAME>>> {
    return this._consumers.values();
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  private _batch: Stream.Batch<VALUE> = [];
  private _batchScheduled = false;

  push(value: VALUE): this {
    this._batch.push(value);

    if (!this._batchScheduled) {
      this._batchScheduled = true;
      queueMicrotask(() => {
        const batch = this._batch;
        this._batch = [];
        this._batchScheduled = false;
        this.batch(batch);
      });
    }
    return this;
  }
  batch(batch: Stream.Batch<VALUE>): this {
    if (!batch.length) return this;
    for (const consumer of this) {
      consumer.batch(batch);
    }
    return this;
  }
  getConsumer(): Consumer<VALUE, Stream.ConsumerName<NAME>> {
    let name: Stream.ConsumerName<NAME>;

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
    this._cleared?.push();
  }
  async dispose() {
    await Promise.all([
      this.clear(),
      this._consumerAttached?.dispose(),
      this._consumerDetached?.dispose(),
      this._cleared?.dispose(),
    ]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this._consumerAttached = this._consumerDetached = this._cleared = this._disposed = undefined;
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
  export type Batch<VALUE> = VALUE[];
  export type ConsumerName<NAME extends string> = `${NAME}Consumer${string}`;
  export type AnyStream = Stream<any, any, any>;
  export type ExtractValue<T> = T extends Stream<infer VALUE, any, any> | Batch<infer VALUE>
    ? VALUE
    : Transformer.ExtractValue<T> extends never
      ? Consumer.ExtractValue<T> extends never
        ? Source.ExtractValue<T> extends never
          ? Queue.ExtractValue<T> extends never
            ? never
            : Queue.ExtractValue<T>
          : Source.ExtractValue<T>
        : Consumer.ExtractValue<T>
      : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;
  export type ExtractError<T> =
    T extends Stream<any, infer ERROR, any>
      ? ERROR
      : Transformer.ExtractError<T> extends never
        ? Source.ExtractError<T> extends never
          ? never
          : Source.ExtractError<T>
        : Transformer.ExtractError<T>;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;
}

// function simpleTest() {
//   const stream = new Stream<number, never>(async function* () {
//     // await new Promise((r) => setTimeout(r, 10));
//     // yield 1 as number;
//     // await new Promise((r) => setTimeout(r, 10));
//     // yield 2;
//     // await new Promise((r) => setTimeout(r, 10));
//     // yield 3;
//   });

//   (async () => {
//     for await (const value of stream) {
//       console.log("c1", value);
//       if (value == 2) break;
//     }
//   })();
//   (async () => {
//     for await (const value of stream) {
//       console.log("c2", value);
//       if (value == 2) break;
//     }
//   })();

//   stream.push(44);
//   stream.push(55);
// }
function bench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number, never>();

  const c = stream[Symbol.asyncIterator]();
  (async () => {
    for await (const items of stream) {
      for (const item of items) {
        item;
        item - 3;
      }
      console.log("bench", items.pop(), Math.round(performance.now() - now));
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}
// function consumerTest() {
//   const stream1 = new Stream("clicks", () => {
//     let i = 0;
//     return {
//       next: async () => {
//         return { value: [i++], done: i > 2 };
//       },
//     };
//   });

//   const consumer1 = stream1.getConsumer();
//   const consumer2 = stream1.getConsumer();

//   (async () => {
//     for await (const value of consumer1) {
//       await new Promise((r) => setTimeout(r, 300));

//       console.log(consumer1.name, value);
//     }
//     console.log(consumer1.name, " done");
//   })();
//   (async () => {
//     for await (const value of consumer2) {
//       console.log(consumer2.name, value);
//     }
//     console.log(consumer2.name, " done");
//   })();

//   // stream1.push(1);
//   // stream1.push(2);
//   // stream.push(3)
// }

// simpleTest();
// bench(); //22ms
// consumerTest();

//generator function latency is 120ms

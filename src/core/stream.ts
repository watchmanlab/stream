import { Channel } from "./channel.ts";
import { Channels } from "./channels.ts";
import { Source } from "./source.ts";
import { Transformer } from "./transformer.ts";

const NAME = "root";

export class Stream<VALUE, ERROR = unknown, NAME extends string = Stream.Name>
  implements AsyncIterable<Stream.Batch<VALUE>>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _channels: Channels<Stream.Batch<VALUE>, NAME>;
  private _source?: Source<VALUE, ERROR, NAME>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;
  constructor(name: NAME, dataGenerator?: Source.DataGenerator<VALUE>);
  constructor(dataGenerator?: Source.DataGenerator<VALUE>);
  constructor(nameOrDataGenerator?: NAME | Source.DataGenerator<VALUE>, dataGenerator?: Source.DataGenerator<VALUE>) {
    if (typeof nameOrDataGenerator === "string") {
      this.name = nameOrDataGenerator;
    } else {
      this.name = NAME as NAME;
      dataGenerator = nameOrDataGenerator;
    }
    if (dataGenerator) this._source = new Source(this, dataGenerator);

    this._channels = new Channels(this);
  }
  [Symbol.asyncIterator](): Channel<Stream.Batch<VALUE>, Channels.ChannelName<NAME>> {
    return this._channels.get();
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
    for (const channel of this._channels) {
      channel.push(batch);
    }
    return this;
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
    await Promise.all([this._channels.dispose(), this._source?.return?.()]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this._source = this._disposed = undefined;
  }

  get channels() {
    return this._channels;
  }
  get source() {
    return this._source;
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
  export type AnyStream = Stream<any, any, any>;
  export type ExtractValue<T extends AnyStream | Transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;
  export type ExtractError<T> =
    T extends Stream<any, infer ERROR, any>
      ? ERROR
      : Transformer.ExtractError<T> extends never
        ? never
        : Transformer.ExtractError<T>;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, any, OUTPUT_NAME>,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;
}

function simpleTest() {
  const stream = new Stream<number, never>(async function* () {
    await new Promise((r) => setTimeout(r, 10));
    yield [1];
    await new Promise((r) => setTimeout(r, 10));
    yield [2];
    await new Promise((r) => setTimeout(r, 10));
    yield [3];
  });

  const v = new Stream(stream);
  //.   ^?

  (async () => {
    for await (const value of v) {
      console.log("c1", value);
      if (value[0] == 2) break;
    }
  })();

  // stream.;
  (async () => {
    for await (const value of v) {
      console.log("c2", value);
      if (value[0] == 2) break;
    }
  })();

  stream.push(44);
  stream.push(55);
}
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

simpleTest();
// bench(); //22ms

import { each } from "../transformers/each.ts";
import { Channels } from "./channels.ts";
import { Source } from "./source.ts";
import { Transformer } from "./transformer.ts";

const NAME = "root";

export class Stream<VALUE, NAME extends string = Stream.Name> implements AsyncDisposable, Disposable {
  readonly name: NAME;
  private _channels: Channels<VALUE>;
  private _source?: Source<VALUE>;
  private _disposed?: Stream<void, `${NAME}Disposed`>;
  constructor(name: NAME, sourceData?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>);
  constructor(sourceData?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>);
  constructor(
    nameOrSourceData?: NAME | Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>,
    sourceData?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>,
  ) {
    if (typeof nameOrSourceData === "string") {
      this.name = nameOrSourceData;
    } else {
      this.name = NAME as NAME;
      sourceData = nameOrSourceData;
    }
    if (sourceData) this._source = new Source(this, sourceData);

    this._channels = new Channels(this);
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
      channel.batch(batch);
    }
    return this;
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
  dispose(): void {
    this._source?.return?.();
    this._channels.dispose();

    this._disposed?.push();
    this._disposed?.dispose();

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
  export type AnyStream = Stream<any, any>;
  export type ExtractValue<T extends AnyStream | Transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, OUTPUT_NAME> | INPUT_STREAM,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;

  export const EMPTY = [];
  export type Empty = typeof EMPTY;
  export function isEmpty<VALUE>(batch: Batch<VALUE>) {
    return !batch.length;
  }
}

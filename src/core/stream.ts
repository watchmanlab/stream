import { Consumer } from "./consumer.ts";
import { Consumers } from "./consumers.ts";
import { Source } from "./source.ts";
import { Transformer } from "./transformer.ts";

const NAME = "root";

export class Stream<VALUE, NAME extends string = Stream.Name> {
  readonly name: NAME;
  private _consumers: Consumers<Stream.Batch<VALUE>>;
  private _source?: Source<VALUE>;

  constructor(name: NAME, source?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>);
  constructor(source?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>);
  constructor(
    nameOrSource?: NAME | Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>,
    source?: Source.SourceData<VALUE> | Source.SourceDataFunction<VALUE>,
  ) {
    if (typeof nameOrSource === "string") {
      this.name = nameOrSource;
    } else {
      this.name = NAME as NAME;
      source = nameOrSource;
    }

    if (source) {
      this._source = new Source({
        sourceData: source,
        next: (value) => {
          this.batch([value]);
        },
        return: () => this._consumers.clear(),
      });
    }

    this._consumers = new Consumers({
      pull: this._source ? () => this._source!.next() : undefined,
    });
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
    for (const consumer of this._consumers) {
      consumer.push(batch);
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

  get consumers() {
    return this._consumers;
  }
  get source() {
    return this._source;
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

  export type Source<VALUE> =
    | Stream<VALUE>
    | AsyncGenerator<VALUE>
    | Generator<VALUE>
    | AsyncIterator<VALUE>
    | Iterator<VALUE>
    | Iterable<VALUE>
    | AsyncIterable<VALUE>;
  export type SourceFunction<VALUE> = () => Source<VALUE>;
  export const EMPTY = [];
  export type Empty = typeof EMPTY;
}

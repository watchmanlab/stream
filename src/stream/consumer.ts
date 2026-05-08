import { Queue } from "./queue.ts";
import { Source } from "./source.ts";
import { Stream } from "./stream.ts";

export class Consumer<VALUE, NAME extends string>
  implements AsyncIterableIterator<Stream.Batch<VALUE>>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _options?: Consumer.Options<VALUE>;
  private _buffer: Queue<VALUE, `${NAME}Buffer`>;
  private _pendings: Queue<(value: Stream.Batch<VALUE> | Queue.Empty) => void, `${NAME}Pending`>;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;

  constructor(name: NAME, options?: Consumer.Options<VALUE>) {
    this.name = name;
    this._options = options;
    this._buffer = new Queue(`${this.name}Buffer`, options?.bufferOptions);
    this._pendings = new Queue(`${this.name}Pending`, options?.pendingsOptions);
  }

  [Symbol.asyncIterator]() {
    return this;
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  push(batch: Stream.Batch<VALUE>): Consumer.PushResult<VALUE, NAME> {
    const pending = this._pendings.dequeue();
    if (pending !== Queue.EMPTY) {
      pending[0](batch);
    } else {
      this._buffer.enqueue(batch);
    }
    const self = this;
    return {
      getProgresses(value: VALUE) {
        return new Consumer.Progress(self.name, value, self);
      },
    };
  }
  getProgress(value: VALUE) {
    return new Consumer.Progress(this.name, value, this);
  }
  private _currentBatch: Stream.Batch<VALUE> | Queue.Empty = Queue.EMPTY;
  async next(): Promise<IteratorResult<Stream.Batch<VALUE>, Queue.Empty>> {
    if (this._currentBatch !== Queue.EMPTY) {
      this._valueProcessed?.pushMany(this._currentBatch);
      this._currentBatch = Queue.EMPTY;
    }

    const batch = this._buffer.dequeue();
    if (batch !== Queue.EMPTY) {
      this._valueProcessing?.pushMany(batch);
      this._currentBatch = batch;

      return { value: batch };
    } else {
      const batch = await new Promise<Stream.Batch<VALUE> | Queue.Empty>((r) => {
        this._pendings.enqueue([r]);
        if (this._options?.source?.idle) this._options.source.requestNext();
      });
      return { value: batch as never, done: batch === Queue.EMPTY };
    }
  }
  async return(): Promise<IteratorReturnResult<Queue.Empty>> {
    for (const [pending] of this._pendings) {
      pending(Queue.EMPTY);
    }

    await Promise.all([
      this._pendings.dispose(),
      this._buffer.dispose(),
      this._valueProcessing?.dispose(),
      this._valueProcessed?.dispose(),
    ]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this._valueProcessing = this._valueProcessed = this._disposed = undefined;

    this._options?.onTerminate?.();
    return { value: Queue.EMPTY as never, done: true };
  }
  async dispose() {
    await this.return();
  }
  get buffer() {
    return this._buffer;
  }
  get pendings() {
    return this._pendings;
  }
  get source() {
    return this._options?.source;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`${this.name}Disposed`);
    return this._disposed;
  }
}
export namespace Consumer {
  export type AnyOptions = Options<any>;
  export type AnyPushProgress = Progress<any, any>;
  export type PushResult<VALUE, NAME extends string> = { getProgresses: (value: VALUE) => Progress<VALUE, NAME> };
  export type ExtractValue<T> = T extends Options<infer VALUE> | Progress<infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyPushProgress ? T["name"] : never;
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
    onTerminate?: () => void;
    bufferOptions?: Queue.Options;
    pendingsOptions?: Queue.Options;
  };

  export class Progress<const VALUE, NAME extends string> {
    private _queued?: Stream<VALUE, never, `${NAME}Queued`>;
    private _processing?: Stream<VALUE, never, `${NAME}Processing`>;
    private _processed?: Stream<VALUE, never, `${NAME}Processed`>;
    private _dropped?: Stream<VALUE, never, `${NAME}Dropped`>;
    constructor(
      public readonly name: NAME,
      private value: VALUE,
      private consumer: Consumer<VALUE, NAME>,
    ) {}

    get queued() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._queued)
        this._queued = new Stream(`${this.name}Queued`, async function* () {
          for await (const batch of consumer.buffer.valueQueued) {
            if (batch.includes(searchValue)) {
              yield [searchValue];
              break;
            }
          }
        });
      return this._queued;
    }
    get processing() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._processing)
        this._processing = new Stream(`${this.name}Processing`, async function* () {
          for await (const batch of consumer.valueProcessing) {
            if (batch.includes(searchValue)) {
              yield [searchValue];
              break;
            }
          }
        });
      return this._processing;
    }
    get processed() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._processed)
        this._processed = new Stream(`${this.name}Processed`, async function* () {
          for await (const batch of consumer.valueProcessed) {
            if (batch.includes(searchValue)) {
              yield [searchValue];
              break;
            }
          }
        });
      return this._processed;
    }
    get dropped() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._dropped)
        this._dropped = new Stream(`${this.name}Dropped`, async function* () {
          for await (const batch of consumer.buffer.valueDropped) {
            if (batch.includes(searchValue)) {
              yield [searchValue];
              break;
            }
          }
        });
      return this._dropped;
    }
  }
}

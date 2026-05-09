import { Queue } from "./queue.ts";
import { Source } from "./source.ts";
import { Stream } from "./stream.ts";

export class Consumer<VALUE, NAME extends string>
  implements AsyncIterableIterator<Stream.Batch<VALUE>>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _source?: Source<VALUE, any, any>;
  private _buffer: Queue<Stream.Batch<VALUE>, `${NAME}Buffer`>;
  private _pendings: Queue<(value: Stream.Batch<VALUE> | Queue.Empty) => void, `${NAME}Pending`>;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;

  constructor(
    name: NAME,
    source: Source<VALUE, any, any> | undefined = undefined,
    private onTerminate: () => void,
  ) {
    this.name = name;
    this._source = source;
    this._buffer = new Queue(`${this.name}Buffer`);
    this._pendings = new Queue(`${this.name}Pending`);
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
  batch(batch: Stream.Batch<VALUE>): this {
    const pending = this._pendings.dequeue();
    if (pending !== Queue.EMPTY) {
      pending(batch);
    } else {
      this._buffer.enqueue(batch);
    }

    return this;
  }

  private _currentBatch: Stream.Batch<VALUE> | Queue.Empty = Queue.EMPTY;
  async next(): Promise<IteratorResult<Stream.Batch<VALUE>, Queue.Empty>> {
    if (this._currentBatch !== Queue.EMPTY) {
      this._valueProcessed?.batch(this._currentBatch);
      this._currentBatch = Queue.EMPTY;
    }

    const batch = this._buffer.dequeue();
    if (batch !== Queue.EMPTY) {
      this._valueProcessing?.batch(batch);
      this._currentBatch = batch;

      return { value: batch };
    } else {
      const batch = await new Promise<Stream.Batch<VALUE> | Queue.Empty>((r) => {
        this._pendings.enqueue(r);
        if (this._source?.idle) this._source.requestNext();
      });
      return { value: batch as never, done: batch === Queue.EMPTY };
    }
  }
  async return(): Promise<IteratorReturnResult<Queue.Empty>> {
    for (const pending of this._pendings) {
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

    this._source = this._valueProcessing = this._valueProcessed = this._disposed = undefined;

    this.onTerminate();
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
    return this._source;
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

  export type ExtractValue<T> = T extends Options<infer VALUE> ? VALUE : never;

  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
    onTerminate?: () => void;
  };
}

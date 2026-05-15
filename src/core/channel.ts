import { Queue } from "./queue.ts";
import { Stream } from "./stream.ts";

export class Channel<VALUE, NAME extends string> implements AsyncIterable<VALUE>, AsyncDisposable, Disposable {
  private _buffer: Queue<Stream.Batch<VALUE>, `${NAME}Buffer`>;
  private _pending?: {
    promise: Promise<Stream.Batch<VALUE>>;
    resolve: (value: Stream.Batch<VALUE>) => void;
  };
  private _valueProcessing?: Stream<VALUE, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, `${NAME}ValueProcessed`>;
  private _done?: Stream<void, `${NAME}Done`>;

  constructor(
    private stream: Stream<VALUE, NAME>,
    private options?: Channel.Options<VALUE>,
  ) {
    this._buffer = new Queue(`${this.stream.name}Buffer`);
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      const batch = await this.next();
      if (!batch.length) break;
      yield* batch;
    }
  }
  async [Symbol.asyncDispose]() {
    await this.return();
  }
  [Symbol.dispose]() {
    this.return();
  }
  batch(batch: Stream.Batch<VALUE>): this {
    if (this._pending) {
      this._pending.resolve(batch);
      this._pending = undefined;
    } else {
      this._buffer.enqueue(batch);
    }
    return this;
  }
  private _currentBatch: Stream.Batch<VALUE> = [];
  next(): Stream.Batch<VALUE> | Promise<Stream.Batch<VALUE>> {
    if (this._currentBatch.length) {
      this._valueProcessed?.batch(this._currentBatch);
      this._currentBatch.length = 0;
    }

    let batch = this._buffer.dequeue();
    if (batch !== Queue.EMPTY) {
      this._valueProcessing?.batch(batch);
      this._currentBatch = batch;
      this.options?.onNext?.(batch);
      return batch;
    }

    return (async () => {
      if (this._pending) {
        batch = await this._pending.promise;
      } else {
        (this._pending as any) = {};

        this._pending!.promise = new Promise<Stream.Batch<VALUE>>((resolve) => {
          this._pending!.resolve = resolve;
          this.stream.source?.pull();
        });

        batch = await this._pending!.promise;
      }

      if (batch.length) {
        this.options?.onNext?.(batch);
        return batch;
      }
      return Stream.EMPTY;
    })();
  }
  async return(): Promise<void> {
    this._pending?.resolve(Stream.EMPTY);

    await Promise.all([this._buffer.dispose(), this._valueProcessing?.dispose(), this._valueProcessed?.dispose()]);

    this._done?.push();
    await this._done?.dispose();

    this.options?.onDone?.();

    this._pending = this._valueProcessing = this._valueProcessed = this._done = undefined;
  }

  get buffer() {
    return this._buffer;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.stream.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.stream.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get done() {
    if (!this._done) this._done = new Stream(`${this.stream.name}Done`);
    return this._done;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    onNext?: (batch: Stream.Batch<VALUE>) => void;
    onDone?: () => void;
  };
}

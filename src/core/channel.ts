import { Queue } from "./queue.ts";
import { Source } from "./source.ts";
import { Stream } from "./stream.ts";

export class Channel<VALUE> implements Source.VoidIterator, Disposable {
  private _buffer: Queue<Stream.Batch<VALUE>>;
  private _pending?: {
    promise: Promise<Stream.Batch<VALUE>>;
    resolve: (value: Stream.Batch<VALUE>) => void;
  };
  private _valueProcessing?: Stream<VALUE, `ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, `ValueProcessed`>;
  private _done?: Stream<void, `Done`>;

  constructor(private options?: Channel.Options<VALUE>) {
    this._buffer = new Queue();
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
  next(): void {
    if (this._currentBatch.length) {
      this._valueProcessed?.batch(this._currentBatch);
      this._currentBatch.length = 0;
    }

    let batch = this._buffer.dequeue();
    if (batch !== Queue.EMPTY) {
      this._valueProcessing?.batch(batch);
      this._currentBatch = batch;
      this.options?.next?.(batch);
      return;
    }

    (async () => {
      if (this._pending) {
        batch = await this._pending.promise;
      } else {
        (this._pending as any) = {};

        this._pending!.promise = new Promise<Stream.Batch<VALUE>>((resolve) => {
          this._pending!.resolve = resolve;
          this.options?.ready?.();
        });

        batch = await this._pending!.promise;
      }

      if (batch.length) this.options?.next?.(batch);
    })();
  }
  return(): void {
    this._pending?.resolve(Stream.EMPTY);
    this._buffer.dispose();
    this._valueProcessing?.dispose();
    this._valueProcessed?.dispose();

    this._done?.push();
    this._done?.dispose();

    this.options?.return?.();

    this._pending = this._valueProcessing = this._valueProcessed = this._done = undefined;
  }
  get buffer() {
    return this._buffer;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`ValueProcessed`);
    return this._valueProcessed;
  }
  get done() {
    if (!this._done) this._done = new Stream(`Done`);
    return this._done;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    next?: (batch: Stream.Batch<VALUE>) => void;
    return?: () => void;
    ready?: () => void;
  };
}

import { Queue } from "./queue.ts";
import { Stream } from "./stream.ts";

const NAME = "channel";
export class Channel<VALUE, NAME extends string = Channel.Name>
  implements AsyncIterable<VALUE>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _buffer: Queue<VALUE, `${NAME}Buffer`>;
  private _pending?: {
    promise: Promise<Stream.Batch<VALUE>>;
    resolve: (value: Stream.Batch<VALUE>) => void;
  };
  private _valueProcessing?: Stream<VALUE, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, `${NAME}ValueProcessed`>;
  private _done?: Stream<void, `${NAME}Done`>;

  constructor(
    name = NAME as NAME,
    private options?: Channel.Options,
  ) {
    this.name = name;
    this._buffer = new Queue(`${this.name}Buffer`);
  }
  [Symbol.asyncIterator]() {
    return {
      next: () => {
        const result = this.next();
        if (result instanceof Promise) {
          return (async () => {
            const batch = await result;

            if (!batch.length) {
              return { value: [], done: true };
            }

            for (let i = 0, length = batch.length; i < length; i++) {
              return { value: batch[i] };
            }
          })();
        } else {
          if (!result.length) {
            return { value: [], done: true };
          }
          for (let i = 0, length = result.length; i < length; i++) {
            return { value: result[i] };
          }
        }
      },
      return: () => this.return(),
    } as AsyncIterator<VALUE>;
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

  private _currentValue: Stream.Batch<VALUE> = [];
  next(
    onValue?: (value: Stream.Batch<VALUE>) => void,
    onDone?: (value: []) => void,
  ): Stream.Batch<VALUE> | Promise<Stream.Batch<VALUE>> {
    if (this._currentValue.length) {
      this._valueProcessed?.batch(this._currentValue);
      this._currentValue.length = 0;
    }

    let batch = this._buffer.dequeue();
    if (batch !== Stream.EMPTY) {
      this._valueProcessing?.batch(batch);
      this._currentValue = batch;
      onValue?.(batch);
      return batch;
    } else {
      return (async () => {
        if (this._pending) {
          batch = await this._pending.promise;
        } else {
          (this._pending as any) = {};

          this._pending!.promise = new Promise<Stream.Batch<VALUE>>((resolve) => {
            this._pending!.resolve = resolve;
            this.options?.pull?.();
          });

          batch = await this._pending!.promise;
        }

        if (!batch.length) {
          onDone?.([]);
          return [];
        }
        onValue?.(batch);
        return batch;
      })();
    }
  }
  async return(): Promise<IteratorReturnResult<[]>> {
    this._pending?.resolve([]);

    await Promise.all([this._buffer.dispose(), this._valueProcessing?.dispose(), this._valueProcessed?.dispose()]);

    this._done?.push();
    await this._done?.dispose();

    this.options?.done?.();

    this._pending = this.options = this._valueProcessing = this._valueProcessed = this._done = undefined;
    return { value: Stream.EMPTY as never, done: true };
  }

  get buffer() {
    return this._buffer;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get done() {
    if (!this._done) this._done = new Stream(`${this.name}Done`);
    return this._done;
  }
}
export namespace Channel {
  export type Name = typeof NAME;

  export type Options = {
    pull?: () => void;
    done?: () => void;
  };
}

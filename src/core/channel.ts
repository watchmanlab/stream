import { Queue } from "./queue.ts";
import { Stream } from "./stream.ts";

const NAME = "channel";
export class Channel<VALUE, NAME extends string = Channel.Name>
  implements AsyncIterableIterator<VALUE>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _buffer: Queue<VALUE, `${NAME}Buffer`>;
  private _pendings: ((value: VALUE | Queue.Empty) => void)[];
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;

  constructor(
    name = NAME as NAME,
    private options?: Channel.Options,
  ) {
    this.name = name;

    this._buffer = new Queue(`${this.name}Buffer`);
    this._pendings = [];
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  async [Symbol.asyncDispose]() {
    await this.return();
  }
  [Symbol.dispose]() {
    this.return();
  }
  push(value: VALUE): this {
    if (this._pendings.length) {
      for (let i = 0, pendings = this._pendings, length = pendings.length; i < length; i++) {
        pendings[i](value);
      }
      this._pendings = [];
    } else {
      this._buffer.enqueue(value);
    }

    return this;
  }

  private _currentValue: VALUE | Queue.Empty = Queue.EMPTY;
  async next(): Promise<IteratorResult<VALUE, Queue.Empty>> {
    if (this._currentValue !== Queue.EMPTY) {
      this._valueProcessed?.push(this._currentValue);
      this._currentValue = Queue.EMPTY;
    }

    const value = this._buffer.dequeue();
    if (value !== Queue.EMPTY) {
      this._valueProcessing?.push(value);
      this._currentValue = value;

      return { value };
    } else {
      const value = await new Promise<VALUE | Queue.Empty>((r) => {
        this._pendings.push(r);
        this.options?.requestNext?.();
      });

      if (value === Queue.EMPTY) return { value: Queue.EMPTY as never, done: true };

      return { value };
    }
  }
  async return(): Promise<IteratorReturnResult<Queue.Empty>> {
    if (this._pendings.length) {
      this.push(Queue.EMPTY as VALUE);
    }

    await Promise.all([this._buffer.dispose(), this._valueProcessing?.dispose(), this._valueProcessed?.dispose()]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this.options?.onDone?.();
    this.options = this._valueProcessing = this._valueProcessed = this._disposed = undefined;
    return { value: Queue.EMPTY as never, done: true };
  }

  get buffer() {
    return this._buffer;
  }
  get pendings() {
    return this._pendings;
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
export namespace Channel {
  export type Name = typeof NAME;

  export type Options = {
    requestNext?: () => void;
    onDone?: () => void;
  };
}

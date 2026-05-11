import { Queue } from "./queue.ts";
import { Stream } from "./stream.ts";

const NAME = "channel";
export class Channel<VALUE, NAME extends string = Channel.Name>
  implements AsyncIterableIterator<VALUE>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _buffer: Queue<VALUE, `${NAME}Buffer`>;
  private _pending?: { promise: Promise<VALUE | Queue.Empty>; resolve: (value: VALUE | Queue.Empty) => void };
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _done?: Stream<void, never, `${NAME}Done`>;

  constructor(
    name = NAME as NAME,
    private options?: Channel.Options,
  ) {
    this.name = name;
    this._buffer = new Queue(`${this.name}Buffer`);
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
    if (this._pending) {
      this._pending.resolve(value);
      this._pending = undefined;
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

    let value = this._buffer.dequeue();
    if (value !== Queue.EMPTY) {
      this._valueProcessing?.push(value);
      this._currentValue = value;

      return { value };
    } else {
      if (this._pending) {
        value = await this._pending.promise;
      } else {
        (this._pending as any) = {};

        this._pending!.promise = new Promise<VALUE | Queue.Empty>((resolve) => {
          this._pending!.resolve = resolve;
          this.options?.pull?.();
        });

        value = await this._pending!.promise;
      }

      if (value === Queue.EMPTY) return { value: Queue.EMPTY as never, done: true };

      return { value };
    }
  }
  async return(): Promise<IteratorReturnResult<Queue.Empty>> {
    this._pending?.resolve(Queue.EMPTY);

    await Promise.all([this._buffer.dispose(), this._valueProcessing?.dispose(), this._valueProcessed?.dispose()]);

    this._done?.push();
    await this._done?.dispose();

    this.options?.done?.();

    this._pending = this.options = this._valueProcessing = this._valueProcessed = this._done = undefined;
    return { value: Queue.EMPTY as never, done: true };
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

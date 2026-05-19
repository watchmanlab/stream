import { Queue } from "./queue.ts";

export class Channel<VALUE> implements Disposable {
  private _buffer: Queue<VALUE>;
  private _pending = false;

  constructor(private options: Channel.Options<VALUE>) {
    this._buffer = options.bufferOptions instanceof Queue ? options.bufferOptions : new Queue(options.bufferOptions);
  }

  [Symbol.dispose]() {
    this.return();
  }
  push(value: VALUE): this {
    if (this._pending) {
      this.options.onNext(value);
      this._pending = false;
    } else {
      this._buffer.enqueue(value);
    }
    return this;
  }

  next(): void {
    let value = this._buffer.dequeue();
    if (value !== Queue.EMPTY) {
      this.options.onNext(value);
      return;
    }

    this._pending = true;
    this.options.onReady?.();
  }
  return(): void {
    this._buffer.clear();
    this.options.onReturn?.();
  }
  get buffer() {
    return this._buffer;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    onNext: (value: VALUE) => void;
    onReturn?: () => void;
    onReady?: () => void;
    bufferOptions?: Queue.Options<VALUE> | Queue<VALUE>;
  };
}

import { Queue } from "./queue.ts";

export class Channel<VALUE> implements Disposable {
  private _queue: Queue<VALUE>;
  private _pending = 0;

  constructor(private options: Channel.Options<VALUE>) {
    this._queue = options.queue ? options.queue : new Queue(options.queueOptions);
  }
  [Symbol.dispose](): void {
    this.return();
  }
  push(value: VALUE): this {
    if (this._pending > 0) {
      this.options.next(value, this);
      this._pending--;
    } else {
      this._queue.enqueue(value);
    }
    return this;
  }
  next(): void {
    let value = this._queue.dequeue();
    if (value !== Queue.EMPTY) {
      this.options.next(value, this);
      return;
    }

    this._pending++;
    this.options.ready?.();
  }
  return(): void {
    this._queue.clear();
    this.options.return?.();
  }
  get queue(): Queue<VALUE> {
    return this._queue;
  }
  get pending(): number {
    return this._pending;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    next: (value: VALUE, channel: Channel<VALUE>) => void;
    return?: () => void;
    ready?: () => void;
  } & ({ queue?: Queue<VALUE>; queueOptions?: never } | { queue?: never; queueOptions?: Queue.Options<VALUE> });
}

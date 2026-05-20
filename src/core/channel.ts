import { Queue } from "./queue.ts";

export class Channel<VALUE> implements Disposable {
  private _queue: Queue<VALUE>;
  private _pending = false;

  constructor(private options: Channel.Options<VALUE>) {
    this._queue = options.queue ? options.queue : new Queue(options.queueOptions);
  }
  [Symbol.dispose]() {
    this.return();
  }
  push(value: VALUE): this {
    if (this._pending) {
      this.options.next(value);
      this._pending = false;
    } else {
      this._queue.enqueue(value);
    }
    return this;
  }
  next(): void {
    let value = this._queue.dequeue();
    if (value !== Queue.EMPTY) {
      this.options.next(value);
      return;
    }

    this._pending = true;
    this.options.ready?.();
  }
  return(): void {
    this._queue.clear();
    this.options.return?.();
  }
  get queue() {
    return this._queue;
  }
  get pending() {
    return this._pending;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    next: (value: VALUE) => void;
    return?: () => void;
    ready?: () => void;
  } & ({ queue?: Queue<VALUE>; queueOptions?: never } | { queue?: never; queueOptions?: Queue.Options<VALUE> });
}

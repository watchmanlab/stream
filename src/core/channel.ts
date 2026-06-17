import { Queue } from "./linked-list.ts";

export class Consumer<VALUE> implements Disposable {
  private _queue: Queue<VALUE>;
  private _pending = 0;

  constructor(private options: Consumer.Options<VALUE>) {
    this._queue = options.queue ? options.queue : new Queue(options.queueOptions);
  }
  [Symbol.dispose](): void {
    this.return();
  }
  push(value: VALUE): void {
    if (this._pending > 0) {
      this._pending--;
      this.options.handler(value, this);
    } else {
      this._queue.enqueue(value);
    }
  }
  next(): void {
    let value = this._queue.dequeue();

    if (value !== Queue.EMPTY) {
      this.options.next(value, this);
      return;
    }

    this._pending++;
    this.options.pull?.();
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

export namespace Consumer {
  export type Options<VALUE> = {
    handler: (value: VALUE, consumer: Consumer<VALUE>) => void;
    return?: () => void;
    pull?: () => void;
  } & ({ queue?: Queue<VALUE>; queueOptions?: never } | { queue?: never; queueOptions?: Queue.Options<VALUE> });
}

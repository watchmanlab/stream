import { Queue } from "./queue.ts";

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
      this.options.next(value, this);
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
    next: (value: VALUE, consumer: Consumer<VALUE>) => void;
    return?: () => void;
    pull?: () => void;
  } & ({ queue?: Queue<VALUE>; queueOptions?: never } | { queue?: never; queueOptions?: Queue.Options<VALUE> });
}

function test() {
  const MAX = 250_000_000;
  const start = performance.now();
  const consumer = new Consumer({
    next(value, consumer) {
      if (value === MAX) console.log(value.toLocaleString("fr"), "ops", Math.round(performance.now() - start), "ms");
      consumer.next();
    },
  });
  consumer.next();
  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// test(); // 300000000 ops 1173 ms

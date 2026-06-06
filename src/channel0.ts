import { Queue } from "./queue.ts";

export class Channel<VALUE> implements Disposable {
  static fromIterable<VALUE>(source: Iterable<VALUE>) {
    const iterator = source[Symbol.iterator]();

    const channel = new Channel<VALUE>({
      next: (value) => {
        //
      },
      pull: () => {
        iterator.next();
      },
    });
  }
  private _queue: Queue<VALUE>;
  private _pending = 0;

  constructor(private options: Channel.Options<VALUE>) {
    this._queue = options.queue ? options.queue : new Queue();
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
    this.options.pull?.(this);
  }
  return(): void {
    this._queue.clear();
    this.options.return?.(this);
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
    next: (value: VALUE, self: Channel<VALUE>) => void;
    return?: (self: Channel<VALUE>) => void;
    pull?: (self: Channel<VALUE>) => void;
    queue?: Queue<VALUE>;
  };
}

function test() {
  const MAX = 250_000_000;
  const start = performance.now();
  const channel = new Channel({
    next(value, channel) {
      if (value === MAX) console.log(value.toLocaleString("fr"), "ops", Math.round(performance.now() - start), "ms");
      channel.next();
    },
  });
  channel.next();
  for (let i = 0; i <= MAX; i++) {
    channel.push(i);
  }
}

// test(); // 300000000 ops 976 ms

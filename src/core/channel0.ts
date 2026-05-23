import { Queue } from "./queue.ts";
import { Consumer as Ch } from "./consumer.ts";

export class Consumer<VALUE> implements Disposable {
  private _done = false;
  private _queue: Queue<VALUE>;
  private _pending: Queue<(value: VALUE) => void>;

  constructor() {
    this._queue = new Queue();
    this._pending = new Queue();
  }

  [Symbol.iterator]() {
    return this;
  }
  [Symbol.dispose](): void {
    this.return();
  }
  push(value: VALUE): void {
    if (this._done) return;

    const pending = this._pending.dequeue();

    if (pending === Queue.EMPTY) {
      this._queue.enqueue(value);
    } else {
      pending(value);
    }
  }
  private _iteratorResult: IteratorResult<VALUE | Promise<VALUE>> = {
    value: Queue.EMPTY as VALUE,
    done: false,
  };
  next() {
    if (this._iteratorResult.done) return this._iteratorResult;

    const value = this._queue.dequeue();

    if (value !== Queue.EMPTY) {
      this._iteratorResult.value = value;
      return this._iteratorResult;
    } else {
      this._iteratorResult.value = new Promise<VALUE>((resolve) => this._pending.enqueue(resolve));
      return this._iteratorResult;
    }
  }
  return() {
    this._done = true;
    this._queue.clear();
    for (const resolve of this._pending) {
      resolve(Queue.EMPTY as VALUE);
    }
    this._iteratorResult.done = true;
    return this._iteratorResult;
  }
  get queue(): Queue<VALUE> {
    return this._queue;
  }
  get pending(): Queue<(value: VALUE) => void> {
    return this._pending;
  }
}

const MAX = 300_000_000;
const start = performance.now();
async function test() {
  const consumer = new Consumer<number>();

  (async () => {
    for (let result of consumer) {
      result = result instanceof Promise ? await result : result;
      if (result === MAX) console.log(result, Math.round(performance.now() - start));
    }
  })();
  for (let i = 0; i <= MAX; i++) {
    consumer.push(i);
  }
}

// test();

import { Mitto } from "./mitto.ts";
import { Queue } from "../queue.ts";

export class Channel<VALUE> implements Disposable {
  private _queue: Queue<VALUE>;
  private _pending: number;
  readonly next: Mitto<VALUE>;
  readonly pull: Mitto<void>;

  constructor(options?: Channel.Options<VALUE>) {
    this._queue = options?.queue ? options.queue : new Queue();
    this._pending = 0;
    this.next = new Mitto();
    this.pull = new Mitto();
  }

  push(value: VALUE): void {
    if (this._pending > 0) {
      this._pending--;
      this.next.emit(value);
    } else {
      this._queue.enqueue(value);
    }
  }

  requestNext() {
    let value = this._queue.dequeue();

    if (value !== Queue.EMPTY) {
      this.next.emit(value);
      return;
    }

    this._pending++;
    this.pull.emit();
  }
  dispose(): void {
    this._queue.clear();
    this.next.clear();
    this.pull.clear();
  }
  [Symbol.dispose](): void {
    this.dispose();
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
    queue?: Queue<VALUE>;
  };
}

function test() {
  const MAX = 100_000_000;
  const start = performance.now();
  const channel = new Channel();
  channel.next.listen((value) => {
    if (value === MAX) {
      console.log(value.toLocaleString("fr"), "ops", Math.round(performance.now() - start), "ms");
      return;
    }
    channel.requestNext();
  });
  channel.requestNext();
  for (let i = 0; i <= MAX; i++) {
    channel.push(i);
  }
}

test(); // 300000000 ops 976 ms

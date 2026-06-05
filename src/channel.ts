import { Queue } from "./queue";
import { Stream } from "./stream";

export class Channel<VALUE> {
  private _queue = new Queue<VALUE>();
  private _pending?: { promise: Promise<VALUE>; resolve: (value: VALUE) => void; reject: () => void };
  private _aborted?: Stream<void, "aborted">;
  constructor(private options?: Channel.Options<Channel<VALUE>>) {}
  push(value: VALUE) {
    if (this._pending) {
      this._pending.resolve(value);
      this._pending = undefined;
    } else {
      this._queue.enqueue(value);
    }
  }

  next(): VALUE | Promise<VALUE> {
    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) return value;

    if (this._pending) return this._pending.promise;

    let resolve!: (value: VALUE) => void;
    let reject!: () => void;

    const promise = new Promise<VALUE>((res, rej) => {
      resolve = res;
      reject = rej;
      this.options?.pull?.(this);
    });

    this._pending = { promise, resolve, reject };
    return promise;
  }
  abort() {
    this._queue.clear();
    this._pending?.reject();
    this.options?.abort?.(this);
    this._aborted?.push();
    this._aborted?.abort();
    this.options = this._aborted = this._pending = undefined;
  }

  get queue() {
    return this._queue;
  }
  get hasPending() {
    return this._pending !== undefined;
  }
  get aborted() {
    if (!this._aborted) this._aborted = new Stream({ name: "aborted" });
    return this._aborted;
  }
}

export namespace Channel {
  export type AnyChannel = Channel<any>;
  export type Options<SELF extends AnyChannel> = {
    pull?: (self: SELF) => void;
    abort?: (self: SELF) => void;
  };

  export const COMPLETED = Symbol.for("completed");
  export type Completed = typeof COMPLETED;
  export const ABORTED = Symbol.for("aborted");
  export type Aborted = typeof ABORTED;
}

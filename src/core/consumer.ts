import { Closable, Empty, EMPTY, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";

export class Consumer<VALUE> implements Closable {
  protected _options: Consumer.Options<VALUE>;
  private _status: Consumer.Status;
  private _queue: Queue<VALUE>;
  private _counter: number;

  private _handler: Consumer.Handler<VALUE>;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._options = { ...options };
    this._status = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._counter = 0;

    this._handler = handler;
  }

  get status() {
    return this._status;
  }
  get queue() {
    return this._queue;
  }

  push(value: VALUE): this {
    const { _queue } = this;
    if (this._counter > 0 && _queue.size === 0) {
      this._handler(this, value);
      this._counter--;
    } else {
      _queue.enqueue(value);
      this._options?.enqueue?.(this, value);
    }
    return this;
  }

  next(): this {
    this._counter++;
    const { _queue } = this;

    if (_queue.size === 0) {
      this._options?.next?.(this);

      return this;
    }

    if (this._counter > 1) return this;

    while (this._counter > 0) {
      const value = _queue.dequeue();
      this._options?.dequeue?.(this, value);

      if (value === EMPTY) {
        if (this._status === "drain") {
          this.terminate("complete");
        } else {
          this._options?.next?.(this);
        }
        break;
      }

      this._handler(this, value);
      this._counter--;
    }
    return this;
  }

  terminate(reason: "abort" | "complete"): this {
    this.push = () => this;
    if (reason === "abort") {
      this.next = this.terminate = () => this;
      this._status = "abort";
      this._queue.clear();
    } else if (this._queue.size) {
      this._status = "drain";
      this._options?.drain?.(this);

      return this;
    } else {
      this.next = this.terminate = () => this;
      this._status = "complete";
    }

    this._options?.terminate?.(this, reason);

    this._handler = () => {};
    this._options = {};

    return this;
  }
}

export namespace Consumer {
  export type Status = "active" | "drain" | "abort" | "complete";

  export type Handler<VALUE> = (self: Consumer<VALUE>, value: VALUE) => void;

  export type Options<VALUE> = {
    queue?: Queue<VALUE>;
    next?: (self: Consumer<VALUE>) => void;
    drain?: (self: Consumer<VALUE>) => void;
    terminate?: (self: Consumer<VALUE>, reason: "abort" | "complete") => void;
    enqueue?: (self: Consumer<VALUE>, value: VALUE) => void;
    dequeue?: (self: Consumer<VALUE>, value: VALUE | Empty) => void;
  };
}

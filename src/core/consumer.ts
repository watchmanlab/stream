import { Empty, EMPTY, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";

export class Consumer<VALUE> {
  protected _options: Consumer.Options<VALUE>;
  private _state: Consumer.State;
  private _queue: Queue<VALUE>;
  private _counter: number;

  private _handler: Consumer.Handler<VALUE>;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._options = { ...options };
    this._state = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._counter = 0;

    this._handler = handler;
  }

  get state() {
    return this._state;
  }
  get queue() {
    return this._queue;
  }

  push(value: VALUE) {
    const { _queue } = this;
    if (this._counter > 0 && _queue.size === 0) {
      this._handler(this, value);
      this._counter--;
    } else {
      _queue.enqueue(value);
      this._options.enqueue?.(this, value);
    }
  }

  next(): void {
    this._counter++;
    const { _queue } = this;

    if (_queue.size === 0) {
      this._options.next?.(this);

      return;
    }

    if (this._counter > 1) return;

    while (this._counter > 0) {
      const value = _queue.dequeue();
      this._options.dequeue?.(this, value);

      if (value === EMPTY) {
        if (this._state === "draining") {
          this.terminate("complete");
        } else {
          this._options.next?.(this);
        }
        break;
      }

      this._handler(this, value);
      this._counter--;
    }
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => this;
    if (reason === "abort") {
      this.next = this.terminate = () => {};
      this._state = "aborted";
      this._queue.clear();
    } else if (this._queue.size) {
      this._state = "draining";
      this._options.drain?.(this);

      return;
    } else {
      this.next = this.terminate = () => {};
      this._state = "completed";
    }

    this._options.terminate?.(this, reason);

    this._handler = () => {};
    this._options = {};
  }
}

export namespace Consumer {
  export type State = "active" | "draining" | "aborted" | "completed";

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

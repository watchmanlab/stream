import { Terminable, Empty, EMPTY, Queue, TerminateReason, TerminableStreamable } from "./types";
import { LinkedListQueue } from "./linked-list-queue";

export class Consumer<VALUE> implements Terminable {
  protected _options: Consumer.Options<VALUE>;
  private _status: Consumer.Status;
  private _queue: Queue<VALUE>;
  private _counter: number;

  private _handler: Consumer.Handler<VALUE>;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    options = { ...options };
    this._status = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._counter = 0;

    this._handler = handler;

    let terminateReason: TerminateReason | undefined = undefined;

    const scopeConsumers: Consumer<TerminateReason>[] = [];

    for (const stream of [...new Set(options.scope)]) {
      if (stream.status === "abort" || stream.status === "complete") {
        terminateReason = stream.status as TerminateReason;
        break;
      }
      scopeConsumers.push(stream.$terminate.consume((_, reason) => this.terminate(reason)).next());
    }

    this._options = terminateReason
      ? {}
      : {
          ...options,
          terminate(self, reason) {
            scopeConsumers.forEach((consumer) => consumer.terminate(reason));
            options.terminate?.(self, reason);
          },
        };

    if (terminateReason) {
      this._status = terminateReason;
      this.push = this.next = this.terminate = this._handler = () => this;
      scopeConsumers.forEach((consumer) => consumer.terminate(terminateReason!));
      scopeConsumers.length = 0;
    }
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

  terminate(reason: TerminateReason): this {
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
  export type Status = "active" | "drain" | TerminateReason;

  export type Handler<VALUE> = (self: Consumer<VALUE>, value: VALUE) => void;

  export type Options<VALUE> = {
    queue?: Queue<VALUE>;
    scope?: TerminableStreamable[];
    next?: (self: Consumer<VALUE>) => void;
    drain?: (self: Consumer<VALUE>) => void;
    terminate?: (self: Consumer<VALUE>, reason: TerminateReason) => void;
    enqueue?: (self: Consumer<VALUE>, value: VALUE) => void;
    dequeue?: (self: Consumer<VALUE>, value: VALUE | Empty) => void;
  };
}

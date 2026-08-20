import type { TerminateReason } from "./types";
import { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue } from "./queue";

export class Consumer<VALUE> implements Disposable {
  private _handler: Consumer.Handler<VALUE>;
  private _options?: Consumer.Options<VALUE>;
  private _status: Consumer.Status;
  private _queue?: Queue<VALUE>;
  private _credit: number;

  private _initCleanup?: Consumer.InitCleanup;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._handler = handler;
    this._options = { ...options, next: options?.passive ? undefined : options?.next };
    this._status = "active";
    this._credit = 0;

    this._initCleanup = this._options?.init?.(this);
  }

  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get status(): Consumer.Status {
    return this._status;
  }
  get queue(): Queue<VALUE> {
    return (this._queue ??= this._options?.queueFactory?.() ?? new DefaultQueue());
  }
  get credit(): number {
    return this._credit;
  }
  push(value: VALUE): this {
    if (this._credit > 0 && !this._queue?.size) {
      this._handler(this, value);
      this._credit--;
    } else {
      (this._queue ??= this._options?.queueFactory?.() ?? new DefaultQueue()).enqueue(value);
      this._options?.enqueue?.(this, value);
    }
    this._options?.push?.(this, value);
    return this;
  }
  next(): this {
    this._credit++;

    if (!this._queue?.size) {
      this._options?.next?.(this);
      return this;
    }

    if (this._credit > 1) return this;

    while (this._credit > 0) {
      const value = this._queue.dequeue();

      if (value === EMPTY) {
        this._queue = undefined;
        if (this._status === "drain") {
          this.terminate("complete");
        } else {
          this._options?.next?.(this);
        }
        break;
      }
      this._options?.dequeue?.(this, value);

      this._handler(this, value);
      this._credit--;
    }
    return this;
  }
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    if (reason === "abort") {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
      this._queue?.clear();
    } else if (this._queue?.size) {
      this._status = "drain";
      this._options?.drain?.(this);
      return this;
    } else {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);

    this._options?.terminate?.(this, reason);

    this._queue = this._options = undefined;

    this._handler = EMPTY_FUNCTION;

    return this;
  }
}

export namespace Consumer {
  export type Status = "active" | "drain" | TerminateReason;
  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;
  export type InitCleanup = (reason: TerminateReason) => void;
  export type Options<VALUE> = {
    passive?: boolean;
    queueFactory?: () => Queue<VALUE>;
    init?: (consumer: Consumer<VALUE>) => undefined | InitCleanup;
    push?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE>) => void;
    drain?: (consumer: Consumer<VALUE>) => void;
    terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
    enqueue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    dequeue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  };
}

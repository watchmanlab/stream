import type { TerminateReason } from "./types";
import { EMPTY, EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue } from "./queue";

export class Consumer<VALUE> implements Disposable {
  protected _options?: Consumer.Options<VALUE>;
  protected _status: Consumer.Status;
  protected _queue?: Queue<VALUE>;
  protected _credit: number;

  constructor(options?: Consumer.Options<VALUE>) {
    this._options = options;
    this._status = "active";
    this._queue = undefined;
    this._credit = 0;
  }
  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get status(): Consumer.Status {
    return this._status;
  }
  get queue(): Queue<VALUE> {
    return (this._queue ??= new DefaultQueue());
  }
  get credit(): number {
    return this._credit;
  }
  push(value: VALUE): this {
    if (this._credit > 0 && !this._queue?.size) {
      this._options?.handler?.(this, value);
      this._credit--;
    } else {
      this.queue.enqueue(value);
      this._options?.enqueue?.(this, value);
    }
    this._options?.push?.(this, value);
    return this;
  }
  next(): this {
    this._credit++;

    if (!this._queue?.size) {
      if (!this._options?.passive) this._options?.next?.(this);
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

      this._options?.handler?.(this, value);
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

    this._options?.terminate?.(this, reason);

    this._queue = this._options = undefined;

    return this;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any>;
  export type Status = "active" | "drain" | TerminateReason;

  export interface Options<VALUE> {
    readonly passive?: boolean;
    handler?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    push?(consumer: Consumer<VALUE>, value: VALUE): void;
    next?(consumer: Consumer<VALUE>): void;
    drain?(consumer: Consumer<VALUE>): void;
    terminate?(consumer: Consumer<VALUE>, reason: TerminateReason): void;
    enqueue?(consumer: Consumer<VALUE>, value: VALUE): void;
    dequeue?(consumer: Consumer<VALUE>, value: VALUE): void;
  }
  export class DefaultOptions<VALUE> implements Required<Options<VALUE>> {
    constructor(protected options?: Options<VALUE>) {}
    get passive(): boolean {
      return false;
    }

    handler(consumer: Consumer<VALUE>, value: VALUE): void {
      return this.options?.handler?.(consumer, value);
    }

    push(consumer: Consumer<VALUE>, value: VALUE): void {
      return this?.options?.push?.(consumer, value);
    }
    next(consumer: Consumer<VALUE>): void {
      return this?.options?.next?.(consumer);
    }
    drain(consumer: Consumer<VALUE>): void {
      return this?.options?.drain?.(consumer);
    }
    enqueue(consumer: Consumer<VALUE>, value: VALUE): void {
      return this?.options?.enqueue?.(consumer, value);
    }
    dequeue(consumer: Consumer<VALUE>, value: VALUE): void {
      return this?.options?.dequeue?.(consumer, value);
    }
    terminate(consumer: Consumer<VALUE>, reason: TerminateReason): void {
      return this?.options?.terminate?.(consumer, reason);
    }
  }
}

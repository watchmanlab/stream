import type { TerminateReason } from "./types";
import { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultQueue } from "./default-queue";
import { Queue } from "./queue";

export class Consumer<VALUE> implements Disposable {
  private _handler: Consumer.Handler<VALUE>;
  private _options: Consumer.Options<VALUE>;
  private _status: Consumer.Status;
  private _queue?: Queue<VALUE>;
  private _credit: number;

  private _initCleanup?: Consumer.InitCleanup;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._handler = handler;
    this._options = options ?? {};
    this._status = "active";
    this._queue = undefined;
    this._credit = 0;

    this._initCleanup = this._options?.init?.(this);
  }

  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get handler(): Consumer.Handler<VALUE> {
    return this._handler;
  }
  set handler(handler: Consumer.Handler<VALUE>) {
    this._handler = handler;
  }
  get options(): Consumer.Options<VALUE> {
    return this._options;
  }
  set options(options: Consumer.Options<VALUE>) {
    this._options = options;
  }
  getOption<NAME extends keyof Consumer.Options<VALUE>>(name: NAME): Consumer.Options<VALUE>[NAME] {
    return this._options[name];
  }
  setOption<NAME extends keyof Consumer.Options<VALUE>>(name: NAME, value: Consumer.Options<VALUE>[NAME]) {
    this._options[name] = value;
  }
  get status(): Consumer.Status {
    return this._status;
  }
  get queue(): Queue<VALUE> | undefined {
    return this._queue;
  }
  get credit(): number {
    return this._credit;
  }

  push(value: VALUE): this {
    if (this._credit > 0 && !this._queue?.size) {
      this._handler(this, value);

      this._credit--;
    } else {
      (this._queue ??= this._options.queueFactory?.() ?? new DefaultQueue()).enqueue(value);
    }

    return this;
  }
  pushMany(...values: [VALUE, ...VALUE[]]): this {
    return this.pushBatch(values);
  }
  pushBatch(values: VALUE[]): this {
    for (let i = 0; i < values.length; i++) {
      this.push(values[i]);
    }
    return this;
  }
  next(): this {
    this._credit++;

    if (!this._queue?.size) {
      this._options.next?.(this);

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
          this._options.next?.(this);
        }
        break;
      }

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
      this._options.drain?.(this);
      return this;
    } else {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);

    this._options.terminate?.(this, reason);

    this._queue = undefined;

    this._options = {};

    this._handler = EMPTY_FUNCTION;

    return this;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any>;
  export type Status = "active" | "drain" | TerminateReason;
  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;
  export type InitCleanup = (reason: TerminateReason) => void;

  export interface Options<VALUE> {
    queueFactory?: () => Queue<VALUE>;
    init?: (consumer: Consumer<VALUE>) => undefined | InitCleanup;
    next?: (consumer: Consumer<VALUE>) => void;
    drain?: (consumer: Consumer<VALUE>) => void;
    terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
  }
}

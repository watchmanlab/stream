import type { Terminable, Queue, TerminateReason, Source } from "./types";
import { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { LinkedListQueue } from "./linked-list-queue";

export class Consumer<VALUE> implements Terminable, Disposable {
  private _handler: Consumer.Handler<VALUE>;
  private _status: Consumer.Status;
  private _queue?: Queue<VALUE>;
  private _credit: number;

  private _initCleanup?: (reason: TerminateReason) => void;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;
  private _queueFactory: () => Queue<VALUE>;
  private _push?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _next?: (consumer: Consumer<VALUE>) => void;
  private _enqueue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _dequeue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _drain?: (consumer: Consumer<VALUE>) => void;
  private _terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    this._handler = handler;
    this._status = "active";
    this._credit = 0;

    this._queueFactory = options?.queueFactory ?? (() => new LinkedListQueue());
    this._push = options?.push;
    this._next = options?.next;
    this._enqueue = options?.enqueue;
    this._dequeue = options?.dequeue;
    this._drain = options?.drain;
    this._terminate = options?.terminate;

    this._sourceConsumer = options?.source?.consume((_, value) => this.push(value), {
      terminate: (_, reason) => this.terminate(reason),
    });
    this._signalConsumer = options?.signal?.consume((_, reason) => this.terminate(reason)).next();

    if (this.status === "active") this._initCleanup = options?.init?.(this);
  }

  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get status(): Consumer.Status {
    return this._status;
  }
  get queue(): Queue<VALUE> {
    return (this._queue ??= this._queueFactory());
  }
  get credit(): number {
    return this._credit;
  }
  push(value: VALUE): this {
    if (this._credit > 0 && !this._queue?.size) {
      this._handler(this, value);
      this._credit--;
    } else {
      (this._queue ??= this._queueFactory()).enqueue(value);
      this._enqueue?.(this, value);
    }
    this._push?.(this, value);
    return this;
  }
  next(): this {
    this._credit++;

    const { _sourceConsumer, _queue, _next, _dequeue } = this;

    if (!_queue?.size) {
      _next?.(this);
      _sourceConsumer?.next();
      return this;
    }

    if (this._credit > 1) return this;

    while (this._credit > 0) {
      const value = _queue.dequeue();

      if (value === EMPTY) {
        this._queue = undefined;
        if (this._status === "drain") {
          this.terminate("complete");
        } else {
          _next?.(this);
        }
        break;
      }
      _dequeue?.(this, value);

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
      this._drain?.(this);
      return this;
    } else {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);
    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);

    this._terminate?.(this, reason);

    this._sourceConsumer =
      this._signalConsumer =
      this._queue =
      this._next =
      this._enqueue =
      this._dequeue =
      this._drain =
      this._terminate =
        undefined;

    this._handler = EMPTY_FUNCTION;

    return this;
  }
}

export namespace Consumer {
  export type Status = "active" | "drain" | TerminateReason;

  export type Handler<VALUE> = (consumer: Consumer<VALUE>, value: VALUE) => void;

  export type Options<VALUE> = {
    source?: Source<VALUE>;
    signal?: Source<TerminateReason>;
    queueFactory?: () => Queue<VALUE>;
    init?: (consumer: Consumer<VALUE>) => undefined | ((reason: TerminateReason) => void);
    push?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE>) => void;
    drain?: (consumer: Consumer<VALUE>) => void;
    terminate?: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;
    enqueue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
    dequeue?: (consumer: Consumer<VALUE>, value: VALUE) => void;
  };
}

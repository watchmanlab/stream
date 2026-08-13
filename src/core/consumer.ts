import type { Terminable, Queue, TerminateReason, Source } from "./types";
import { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { LinkedListQueue } from "./linked-list-queue";

export class Consumer<VALUE> implements Terminable, Disposable {
  private _queueFactory: () => Queue<VALUE>;
  private _push: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _next: (consumer: Consumer<VALUE>) => void;
  private _enqueue: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _dequeue: (consumer: Consumer<VALUE>, value: VALUE) => void;
  private _drain: (consumer: Consumer<VALUE>) => void;
  private _terminate: (consumer: Consumer<VALUE>, reason: TerminateReason) => void;

  private _initCleanup?: (reason: TerminateReason) => void;
  private _status: Consumer.Status;
  private _queue?: Queue<VALUE>;
  private _credit: number;

  private _handler: Consumer.Handler<VALUE>;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  constructor(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>) {
    const { source, signal, queueFactory, init, push, next, enqueue, dequeue, drain, terminate } = options ?? {};

    this._queueFactory = queueFactory ?? (() => new LinkedListQueue());
    this._push = push ?? EMPTY_FUNCTION;
    this._next = next ?? EMPTY_FUNCTION;
    this._enqueue = enqueue ?? EMPTY_FUNCTION;
    this._dequeue = dequeue ?? EMPTY_FUNCTION;
    this._drain = drain ?? EMPTY_FUNCTION;
    this._terminate = terminate ?? EMPTY_FUNCTION;
    this._handler = handler;

    this._status = "active";
    this._credit = 0;

    this._sourceConsumer = source?.consume((_, value) => this.push(value), {
      terminate: (_, reason) => this.terminate(reason),
    });
    this._signalConsumer = signal?.consume((_, reason) => this.terminate(reason)).next();

    if (this.status === "active") this._initCleanup = init?.(this);
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
      this._enqueue(this, value);
    }
    this._push(this, value);
    return this;
  }
  next(): this {
    this._credit++;

    const { _sourceConsumer, _queue, _next, _dequeue } = this;

    if (!_queue?.size) {
      _next(this);
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
          _next(this);
        }
        break;
      }
      _dequeue(this, value);

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
      this._drain(this);
      return this;
    } else {
      this.next = this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);
    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);
    this._sourceConsumer = this._signalConsumer = this._queue = undefined;

    this._terminate(this, reason);

    this._handler = this._next = this._enqueue = this._dequeue = this._drain = this._terminate = EMPTY_FUNCTION;

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

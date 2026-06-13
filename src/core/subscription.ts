import { Queue } from "../queue";

export class Subscription<VALUE> implements Subscription.Executor<VALUE> {
  private _isReady: boolean = true;
  private _queue: Queue<VALUE>;
  private _isProcessing = false;
  private _value: VALUE = undefined as VALUE;
  constructor(private init: Subscription.Init<VALUE>) {
    this._queue = new Queue();
    this.ready = this.ready.bind(this);
    this.abort = this.abort.bind(this);
  }
  get value() {
    return this._value;
  }

  push(value: VALUE) {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;
      this._value = value;
      try {
        this.init.listener(this);
      } finally {
        this._isProcessing = false;
      }

      if (this._isReady) {
        this.drain();
      }
    } else {
      this._queue.enqueue(value);
    }
  }

  ready() {
    if (this._isReady) return;
    this._isReady = true;
    this.drain();
  }
  private drain() {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        this._value = this._queue.dequeue() as VALUE;

        this.init.listener(this);
      }
    } finally {
      this._isProcessing = false;
    }
  }
  abort() {
    this._value = undefined as VALUE;
    this._queue.clear();
    this.init.onAbort();
    this.init = {} as Subscription.Init<VALUE>;
  }
}

export namespace Subscription {
  export type Abort = () => void;
  export type Ready = () => void;
  export interface Executor<VALUE> {
    readonly value: VALUE;
    readonly ready: Ready;
    readonly abort: Abort;
  }
  export type Listener<VALUE> = (executor: Executor<VALUE>) => void;

  export type Init<VALUE> = {
    listener: Subscription.Listener<VALUE>;
    onAbort: () => void;
  };
}

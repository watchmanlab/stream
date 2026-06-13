import { Queue } from "../queue";

export class Subscription<VALUE> {
  private _isReady: boolean = true;
  private _queue: Queue<VALUE>;
  private _isProcessing = false;

  constructor(private init: Subscription.Init<VALUE>) {
    this._queue = new Queue();
  }

  push(value: VALUE) {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this.init.listener({ value, ready: this.ready, abort: this.abort });
      } finally {
        this._isProcessing = false;
      }

      //   if (this._isReady && this._queue.size) {
      //     this.drain();
      //   }
    } else {
      this._queue.enqueue(value);
    }
  }

  private drain() {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this.init.listener({ value, ready: this.ready, abort: this.abort });
      }
    } finally {
      this._isProcessing = false;
    }
  }
  readonly ready = (): void => {
    if (this._isReady) return;
    this._isReady = true;
    this.drain();
  };

  readonly abort = (): void => {
    this._queue.clear();
    this.init.onAbort();
    this.init = {} as Subscription.Init<VALUE>;
  };
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

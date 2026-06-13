import { Queue } from "./queue";
import type { IQueue } from "./types";
import type { Vapor } from "./vapor";

export class Subscription<VALUE, ERROR> {
  private _isReady: boolean = true;
  private _queue: IQueue<VALUE>;
  private _isProcessing = false;

  constructor(private init: Subscription.Init<VALUE, ERROR>) {
    this._queue = init.queue ? init.queue : new Queue();
    this.initReady();
  }

  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this.init.listener({ value, ready: this.ready, abort: this.abort });
      } catch (error) {
        this.error(error);
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }

  private drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this.init.listener({ value, ready: this.ready, abort: this.abort });
      }
    } catch (error) {
      this.error(error);
    } finally {
      this._isProcessing = false;
    }
  }
  private error(error: any): void {
    if (!this.init.globalError?.subscriptionsCount && !this.init.error) {
      Promise.reject(error);
    } else {
      this.init.globalError?.emit(error);
      this.init.error?.(error);
    }
  }
  readonly ready: Subscription.Ready<ERROR> = (error?: ERROR): void => {
    if (this._isReady) return;
    if (error) this.error(error);
    this.init.ready!(error);
    this._isReady = true;
    this.drain();
  };
  private initReady(): void {
    if (!this.init.ready)
      (this.ready as any) = (error?: ERROR): void => {
        if (this._isReady) return;
        if (error) this.error(error);
        this._isReady = true;
        this.drain();
      };
  }

  readonly abort: Subscription.Abort<ERROR> = (error?: ERROR): void => {
    this._queue.clear();
    this.init.abort?.(error);
    this.init = {} as Subscription.Init<VALUE, any>;
    if (error) this.error(error);
  };
}

export namespace Subscription {
  export type Abort<ERROR> = (error?: ERROR) => void;
  export type Ready<ERROR> = (error?: ERROR) => void;
  export type Error<ERROR> = (error: ERROR) => void;

  export type Executor<VALUE, ERROR> = {
    readonly value: VALUE;
    readonly ready: Ready<ERROR>;
    readonly abort: Abort<ERROR>;
  };
  export type Listener<VALUE, ERROR> = (executor: Executor<VALUE, ERROR>) => void;
  export type Init<VALUE, ERROR> = {
    listener: Subscription.Listener<VALUE, ERROR>;
    ready?: Ready<ERROR>;
    abort?: Abort<ERROR>;
    error?: Error<ERROR>;
    queue?: IQueue<VALUE>;
    globalError?: Vapor<any>;
  };
  export type State = "active" | "drain" | "complete" | "abort";
}

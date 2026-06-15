import type { Queue } from "./types";
import type { Smoker } from "./smoker";
import { LinkedList } from "./queue";

export class Consumer<VALUE, ERROR> {
  private _isReady: boolean;
  private _queue: Queue<VALUE>;
  private _isProcessing = false;

  constructor(private init: Consumer.Init<VALUE, ERROR>) {
    this._queue = init.queue ? init.queue : new LinkedList();
    this._isReady = init.isReady === undefined ? true : init.isReady;
  }
  push(value: VALUE) {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this.init.handler(value, this);
      } catch (error) {
        this.error(error);
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR) {
    if (this._isReady) return;
    if (error) this.error(error);
    this._isReady = true;
    if (!this._queue.size) this.init.ready?.();
    this.drain();
  }
  abort(error?: ERROR) {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this.init.abort?.(error);
    this.init = null as any;

    if (error) this.error(error);
  }
  private drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this.init.handler(value, this);
      }
    } catch (error) {
      this.error(error);
    } finally {
      this._isProcessing = false;
    }
  }
  private error(error: any): void {
    if (!this.init.globalError?.consumers && !this.init.error) {
      Promise.reject(error);
    } else {
      this.init.globalError?.emit(error);
      this.init.error?.(error);
    }
  }
  get queue(): Queue<VALUE> {
    return this._queue;
  }
  get isReady(): boolean {
    return this._isReady;
  }
  get isProcessing(): boolean {
    return this._isProcessing;
  }
}

export namespace Consumer {
  export type Push<VALUE> = (value: VALUE) => void;
  export type Abort<ERROR> = (error?: ERROR, drain?: boolean) => void;
  export type Ready = () => void;
  export type Error<ERROR> = (error: ERROR) => void;
  export type Handler<VALUE, ERROR> = (value: VALUE, consumer: Consumer<VALUE, ERROR>) => void;
  export type Init<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    ready?: Ready;
    abort?: Abort<ERROR>;
    error?: Error<ERROR>;
    queue?: Queue<VALUE>;
    globalError?: Smoker<any>;
    isReady?: boolean;
  };
}

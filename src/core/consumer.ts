import { LinkedList } from "./queue";
import type { Queue } from "./types";
import type { Smoker } from "./smoker";

export class Consumer<VALUE, ERROR> {
  private _isReady: boolean;
  private _queue: Queue<VALUE>;
  private _isProcessing = false;

  constructor(private init: Consumer.Init<VALUE, ERROR>) {
    this._queue = init.queue ? init.queue : new LinkedList();
    this._isReady = init.isReady === undefined ? true : init.isReady;
    this.initReady();
  }

  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this.init.handler(value, this.ready, this.abort);
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

        this.init.handler(value, this.ready, this.abort);
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
  private initReady(): void {
    if (!this.init.ready)
      (this.ready as any) = (error?: ERROR): void => {
        if (this._isReady) return;
        if (error) this.error(error);
        this._isReady = true;
        this.drain();
      };
  }
  readonly ready: Consumer.Ready<ERROR> = (error?: ERROR): void => {
    if (this._isReady) return;
    if (error) this.error(error);
    this.init.ready!(error);
    this._isReady = true;
    this.drain();
  };

  readonly abort: Consumer.Abort<ERROR> = (error?: ERROR): void => {
    this._queue.clear();
    this.init.abort?.(error);
    this.init = {} as Consumer.Init<VALUE, any>;
    if (error) this.error(error);
  };
}

export namespace Consumer {
  export type Abort<ERROR> = (error?: ERROR) => void;
  export type Ready<ERROR> = (error?: ERROR) => void;
  export type Error<ERROR> = (error: ERROR) => void;

  export type Executor<VALUE, ERROR> = {
    readonly value: VALUE;
    readonly ready: Ready<ERROR>;
    readonly abort: Abort<ERROR>;
  };
  export type Handler<VALUE, ERROR> = (value: VALUE, ready: Ready<ERROR>, abort: Abort<ERROR>) => void;
  export type Init<VALUE, ERROR> = {
    handler: Consumer.Handler<VALUE, ERROR>;
    ready?: Ready<ERROR>;
    abort?: Abort<ERROR>;
    error?: Error<ERROR>;
    queue?: Queue<VALUE>;
    globalError?: Smoker<any>;
    isReady?: boolean;
  };
  export type State = "active" | "drain" | "complete" | "abort";
}

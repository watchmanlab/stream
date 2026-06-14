import { LinkedList } from "./queue";
import type { Queue, Source } from "./types";
import type { Smoker } from "./smoker";

export class Consumer<VALUE, ERROR> {
  private _isReady: boolean;
  private _queue: Queue<VALUE>;
  private _isProcessing = false;

  constructor(private init: Consumer.Init<VALUE, ERROR>) {
    this._queue = init.queue ? init.queue : new LinkedList();
    this._isReady = init.isReady === undefined ? true : init.isReady;
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
  readonly ready: Source.Ready<ERROR> = (error?: ERROR): void => {
    if (this._isReady) return;
    if (error) this.error(error);
    this._isReady = true;
    if (!this._queue.size) this.init.ready?.();
    this.drain();
  };

  readonly abort: Source.Abort<ERROR> = (error?: ERROR): void => {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this.init.abort?.(error);
    this.init = null as any;
    this.drain = null as any;
    (this.ready as any) = null as any;
    (this.abort as any) = null as any;
    if (error) this.error(error);
  };
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
}

export namespace Consumer {
  export type Init<VALUE, ERROR> = Source.ListenInit<VALUE, ERROR> & {
    queue?: Queue<VALUE>;
    globalError?: Smoker<any>;
    isReady?: boolean;
  };
}

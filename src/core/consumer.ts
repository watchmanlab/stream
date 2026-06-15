import type { Queue } from "./types";
import type { Smoker } from "./smoker";
import { LinkedList } from "./queue";

export class Consumer<VALUE, ERROR> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;

  constructor(private init: Consumer.Init<VALUE, ERROR>) {
    this._queue = init.queue ? init.queue : new LinkedList();
    this._isReady = init.isReady === undefined ? true : init.isReady;
    this._isProcessing = false;
    this._state = "active";
  }
  push(value: VALUE): void {
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
  next(error?: ERROR): void {
    if (this._isReady) return;
    if (error) this.error(error);
    this._isReady = true;

    if (this._queue.size === 0) {
      if (this._state === "active") {
        this.init.ready?.();
      } else {
        this._state = "completed";
        this.clean();
        this.init.complete?.();
      }
    }
    this.drain();
  }
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this.error(new Consumer.Exception("push_not_allowed", "aborted"));
    this.clean();
    this.init.abort?.(error);
    if (error) this.error(error);
  }

  complete(): void {
    if (this._state !== "active") return;
    if (this._queue.size) {
      this._state = "drain";
      this.init.drain?.();
      this.push = () => this.error(new Consumer.Exception("push_not_allowed", "drain"));
    } else {
      this._state = "completed";
      this.clean();
      this.init.complete?.();
      this.push = () => this.error(new Consumer.Exception("push_not_allowed", "completed"));
    }
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
  private clean() {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this.init = null as any;
  }
  private error(error: any): void {
    if (!this.init.globalError?.get("consumersCount") && !this.init.error) {
      Promise.reject(error);
    } else {
      this.init.globalError?.emit(error);
      this.init.error?.(error);
    }
  }
  get state() {
    return this._state;
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
  export type State = "active" | "drain" | "aborted" | "completed";
  export type Push<VALUE> = (value: VALUE) => void;
  export type Ready = () => void;
  export type Complete = () => void;
  export type Drain = () => void;
  export type Abort<ERROR> = (error?: ERROR) => void;

  export type Error<ERROR> = (error: ERROR) => void;
  export type Handler<VALUE, ERROR> = (value: VALUE, consumer: Consumer<VALUE, ERROR>) => void;
  export type Init<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    ready?: Ready;
    drain?: Drain;
    complete?: Complete;
    abort?: Abort<ERROR>;
    error?: Error<ERROR>;
    queue?: Queue<VALUE>;
    globalError?: Smoker<any>;
    isReady?: boolean;
  };

  export class Exception extends Error {
    override cause?: "aborted" | "completed" | "drain";
    override message: "push_not_allowed";
    constructor(message: "push_not_allowed", cause: "aborted" | "completed" | "drain") {
      super();

      this.cause = cause;
      this.message = message;
    }
  }
}

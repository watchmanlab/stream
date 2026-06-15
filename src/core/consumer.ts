import type { Queue } from "./types";
import { Smoker } from "./smoker";
import { LinkedList } from "./queue";

export class Consumer<VALUE, ERROR> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;
  private _handler: Consumer.Handler<VALUE, ERROR>;
  private _ready?: Consumer.Ready;
  private _drain?: Consumer.Drain;
  private _complete?: Consumer.Complete;
  private _abort?: Consumer.Abort<ERROR>;
  private _error?: Consumer.Error<ERROR>;
  private _globalError?: Smoker<any>;
  constructor(init: Consumer.Init<VALUE, ERROR>) {
    this._handler = init.handler;
    this._ready = init.ready;
    this._drain = init.drain;
    this._complete = init.complete;
    this._abort = init.abort;
    this._error = init.error;
    this._queue = init.queue ? init.queue : new LinkedList();
    this._globalError = init.globalError;
    this._isReady = init.isReady === undefined ? true : init.isReady;

    this._isProcessing = false;
    this._state = "active";
  }
  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this._handler(value, this);
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
        this._ready?.();
      } else {
        this._state = "completed";
        this.clean();
        this._complete?.();
      }
    }
    this.drain();
  }
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this.error(new Smoker.Exception("push_not_allowed", "aborted"));
    this.clean();
    this._abort?.(error);
    if (error) this.error(error);
  }

  complete(): void {
    if (this._state !== "active") return;
    if (this._queue.size) {
      this._state = "drain";
      this._drain?.();
      this.push = () => this.error(new Smoker.Exception("push_not_allowed", "drain"));
    } else {
      this._state = "completed";
      this.clean();
      this._complete?.();
      this.push = () => this.error(new Smoker.Exception("push_not_allowed", "completed"));
    }
  }
  private drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this._handler(value, this);
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
    (this._handler as any) =
      this._ready =
      this._drain =
      this._complete =
      this._abort =
      this._error =
      this._globalError =
        undefined;
  }
  private error(error: any): void {
    if (!this._globalError?.get("consumersCount") && !this._error) {
      Promise.reject(error);
    } else {
      this._globalError?.push(error);
      this._error?.(error);
    }
  }

  get<PROP extends "state" | "queue" | "isReady" | "isProcessing">(
    prop: PROP,
  ): PROP extends "state"
    ? Consumer.State
    : PROP extends "queue"
      ? Queue<VALUE>
      : PROP extends "isReady" | "isProcessing"
        ? boolean
        : never {
    switch (prop) {
      case "state":
        return this._state as never;
      case "queue":
        return this._queue as never;
      case "isReady":
        return this._isReady as never;
      case "isProcessing":
        return this._isProcessing as never;
    }
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
}

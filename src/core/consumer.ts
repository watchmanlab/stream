import type { Queue, Source } from "./types";
import { LinkedList } from "./linked-list";

export class Consumer<VALUE, ERROR = never> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;
  private _init?: Consumer.Init<VALUE, ERROR>;

  constructor(init: Consumer.Init<VALUE, ERROR>, mergeInit?: Partial<Consumer.Init<VALUE, ERROR>>) {
    this._init = {
      ...init,
      ...Object.entries(mergeInit ?? {}).map(([key, val]) =>
        typeof val === "function"
          ? [key, (...args: any) => ((val as Function)(...args), (init as any)[key](...args))]
          : [key, val],
      ),
    };

    this._queue = this._init.queue ? this._init.queue : new LinkedList();
    this._isReady = this._init.isReady === undefined ? true : this._init.isReady;
    this._isProcessing = false;
    this._state = "active";
  }

  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this._init?.handler(this, value);
      } catch (error: any) {
        this._init?.error?.(this, error);
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR): void {
    if (error && this._init?.error) this._init.error(this, error);

    if (this._isReady) {
      this._init?.ready?.(this);
      return;
    }
    this._isReady = true;

    if (this._queue.size === 0) {
      try {
        if (this._state === "active") {
          this._init?.ready?.(this);
        } else {
          this._state = "completed";
          this._init?.complete?.(this);
          this.clean();
        }
      } catch (error: any) {
        this._init?.error?.(this, error);
      }
    }
    this.drain();
  }
  private drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;

    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this._init?.handler(this, value);
      }
    } catch (error: any) {
      this._init?.error?.(this, error);
    } finally {
      this._isProcessing = false;
    }
  }
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";

    try {
      this._init?.abort?.(this, error);
    } catch (error: any) {
      this._init?.error?.(this, error);
    } finally {
      this.clean();
    }
  }
  complete(): void {
    if (this._state !== "active") return;
    try {
      if (this._queue.size) {
        this._state = "drain";
        this.push = () => {};
        this._init?.drain?.(this);
      } else {
        this._state = "completed";
        this._init?.complete?.(this);
      }
    } catch (error: any) {
      this._init?.error?.(this, error);
    } finally {
      this.clean();
    }
  }
  private clean() {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this._init = null!;
    this.push = () => {};
  }
  get init() {
    return { ...this._init };
  }
  get state() {
    return this._state;
  }
  get queue() {
    return this._queue;
  }
  get isReady() {
    return this._isReady;
  }
  get isProcessing() {
    return this._isProcessing;
  }
}

export namespace Consumer {
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, ERROR> = (self: Consumer<VALUE, ERROR>, value: VALUE) => void;

  export type Init<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    ready?: (self: Consumer<VALUE, ERROR>) => void;
    complete?: (self: Consumer<VALUE, ERROR>) => void;
    drain?: (self: Consumer<VALUE, ERROR>) => void;
    abort?: (self: Consumer<VALUE, ERROR>, error?: ERROR) => void;
    error?: (self: Consumer<VALUE, ERROR>, error: ERROR) => void;
    queue?: Queue<VALUE>;
    isReady?: boolean;
  };

  export class AbortException {
    private _abortException = Symbol.for("AbortException");
    constructor(public readonly error?: any) {}
  }
  export class CompleteException {
    private _completeExceptionBrand = Symbol.for("CompleteException");
  }
}

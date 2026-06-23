import type { Queue } from "./types";
import { LinkedList } from "./linked-list";
import { Stream } from "./stream";

export class Consumer<VALUE, ERROR = never> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;
  private _options: Consumer.Options<VALUE, ERROR>;
  private _handler: Consumer.Handler<VALUE, ERROR>;
  private _ready: (self: Consumer<VALUE, ERROR>) => void;
  constructor(handler: Consumer.Handler<VALUE, ERROR>, options: Consumer.Options<VALUE, ERROR>) {
    this._options = { ...options };
    this._handler = handler;
    this._ready = options.ready ?? (() => {});

    this._queue = this._options.queue ? this._options.queue : new LinkedList();
    this._isReady = this._options.isReady === undefined ? true : this._options.isReady;
    this._isProcessing = false;
    this._state = "active";
  }

  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this._handler(this, value);
      } catch (error: any) {
        this._options?.error?.(this, error);
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR): void {
    if (error && this._options?.error) this._options.error(this, error);

    if (this._isReady) {
      this._ready(this);
      return;
    }
    this._isReady = true;

    if (this._queue.size === 0) {
      try {
        if (this._state === "active") {
          this._ready(this);
        } else {
          this._state = "completed";
          this._options?.complete?.(this);
          this.clean();
        }
      } catch (error: any) {
        this._options?.error?.(this, error);
      }
    } else {
      this._drain();
    }
  }
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";

    try {
      this._options?.abort?.(this, error);
    } catch (error: any) {
      this._options?.error?.(this, error);
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
        this._options?.drain?.(this);
      } else {
        this._state = "completed";
        this._options?.complete?.(this);
      }
    } catch (error: any) {
      this._options?.error?.(this, error);
    } finally {
      this.clean();
    }
  }
  private _drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;
    try {
      while (this._isReady && this._queue.size) {
        this._isReady = false;

        const value = this._queue.dequeue() as VALUE;

        this._handler(this, value);
      }
    } catch (error: any) {
      this._options?.error?.(this, error);
    } finally {
      this._isProcessing = false;
    }
  }
  private clean(): void {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this._options = null!;
    this.push = this.next = this._drain = () => {};
  }
  get state(): Consumer.State {
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
  get handler(): Consumer.Handler<VALUE, ERROR> {
    return this._handler;
  }
}

export namespace Consumer {
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, ERROR> = (self: Consumer<VALUE, ERROR>, value: VALUE) => void;

  export type Options<VALUE, ERROR> = {
    ready?: (self: Consumer<VALUE, ERROR>) => void;
    drain?: (self: Consumer<VALUE, ERROR>) => void;
    complete?: (self: Consumer<VALUE, ERROR>) => void;
    abort?: (self: Consumer<VALUE, ERROR>, error?: ERROR) => void;
    error?: (self: Consumer<VALUE, ERROR>, error: ERROR) => void;
    queue?: Queue<VALUE>;
    isReady?: boolean;
  };

  export type Events<VALUE, NAME extends string> = {
    ready: Stream<Consumer<VALUE, any>, `${NAME}Ready`>;
    drain: Stream<void, `${NAME}Drain`>;
    complete: Stream<void, `${NAME}Complete`>;
    abort: Stream<any, `${NAME}Abort`>;
    error: Stream<any, `${NAME}Error`>;
  };
}

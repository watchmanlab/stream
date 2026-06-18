import type { EventShape, Queue } from "./types";
import { Stream } from "./stream";
import { LinkedList } from "./linked-list";

export class Consumer<VALUE, ERROR> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;
  private _handler: Consumer.Handler<VALUE, ERROR>;
  private _fireEvent: Consumer.EventsHandler;

  constructor(init: Consumer.Init<VALUE, ERROR>) {
    this._handler = init.handler;
    this._fireEvent = init.event ?? (() => {});

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
        this._handler(value, this);
      } catch (error) {
        this._fireEvent({ type: "error", error });
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR): void {
    this._fireEvent({ type: "next", error });
    if (error) this._fireEvent({ type: "error", error });

    if (this._isReady) return;
    this._isReady = true;

    if (this._queue.size === 0) {
      if (this._state === "active") {
        this._fireEvent({ type: "ready" });
      } else {
        this._state = "completed";
        this.clean();
        this._fireEvent({ type: "complete" });
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

        this._handler(value, this);
      }
    } catch (error: any) {
      this._fireEvent({ type: "error", error });
    } finally {
      this._isProcessing = false;
    }
  }

  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this._fireEvent({ type: "error", error: new Stream.Exception("push_not_allowed", "aborted") });

    this.clean();
    this._fireEvent({ type: "abort", error });
    if (error) this._fireEvent({ type: "error", error });
  }
  complete(): void {
    if (this._state !== "active") return;
    if (this._queue.size) {
      this._state = "drain";
      this._fireEvent({ type: "drain" });

      this.push = () => this._fireEvent({ type: "error", error: new Stream.Exception("push_not_allowed", "drain") });
    } else {
      this._state = "completed";
      this.clean();
      this._fireEvent({ type: "complete" });
      this.push = () =>
        this._fireEvent({ type: "error", error: new Stream.Exception("push_not_allowed", "completed") });
    }
  }
  private clean() {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    (this._handler as any) = undefined;
  }
  get handler() {
    return this._handler;
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
  export type Handler<VALUE, ERROR> = (value: VALUE, consumer: Consumer<VALUE, ERROR>) => void;
  export type Event =
    | EventShape<"ready" | "complete" | "drain">
    | EventShape<"next" | "abort" | "error", { error?: any }>;
  export type EventsHandler = (event: Event) => void;
  export type Init<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    event?: EventsHandler;
    queue?: Queue<VALUE>;
    isReady?: boolean;
  };
}

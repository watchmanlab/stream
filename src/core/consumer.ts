import type { EventShape, Queue } from "./types";
import { Smoker } from "./smoker";
import { LinkedList } from "./linked-list";

export class Consumer<VALUE, ERROR> {
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;

  private _state: Consumer.State;
  private _handler: Consumer.Handler<VALUE, ERROR>;
  private _fireEvent!: Consumer.OnEvent<VALUE>;
  private _event?: Smoker<Consumer.Event<VALUE>, `consumerEvent`>;
  constructor(init: Consumer.Init<VALUE, ERROR>) {
    this._handler = init.handler;

    this.bindEvent(init.onEvent);

    this._queue = init.queue ? init.queue : new LinkedList();
    this._isReady = init.isReady === undefined ? true : init.isReady;
    this._isProcessing = false;
    this._state = "active";
  }
  private bindEvent(onEvent?: Consumer.OnEvent<VALUE>) {
    this._fireEvent = onEvent
      ? (event: Consumer.Event<VALUE>) => {
          onEvent(event);
          this._event?.push(event);
        }
      : (event: Consumer.Event<VALUE>) => this._event?.push(event);
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
    if (this._isReady) return;
    this._isReady = true;
    if (error) this._fireEvent({ type: "error", error });

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
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";
    this.push = () => this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "aborted") });

    this.clean();
    this._fireEvent({ type: "abort", error });
    if (error) this._fireEvent({ type: "error", error });
  }
  complete(): void {
    if (this._state !== "active") return;
    if (this._queue.size) {
      this._state = "drain";
      this._fireEvent({ type: "drain" });

      this.push = () => this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "drain") });
    } else {
      this._state = "completed";
      this.clean();
      this._fireEvent({ type: "complete" });
      this.push = () =>
        this._fireEvent({ type: "error", error: new Smoker.Exception("push_not_allowed", "completed") });
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
    } catch (error: any) {
      this._fireEvent({ type: "error", error });
    } finally {
      this._isProcessing = false;
    }
  }
  private clean() {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
    this._event?.complete();
    (this._handler as any) = this._event = undefined;
  }

  get<PROP extends "handler" | "state" | "queue" | "isReady" | "isProcessing" | "event">(
    prop: PROP,
  ): PROP extends "handler"
    ? Consumer.Handler<VALUE, ERROR>
    : PROP extends "state"
      ? Consumer.State
      : PROP extends "queue"
        ? Queue<VALUE>
        : PROP extends "isReady" | "isProcessing"
          ? boolean
          : PROP extends "event"
            ? Smoker<Consumer.Event<VALUE>, `consumerEvent`>
            : never {
    switch (prop) {
      case "handler":
        return this._handler as never;
      case "state":
        return this._state as never;
      case "queue":
        return this._queue as never;
      case "isReady":
        return this._isReady as never;
      case "isProcessing":
        return this._isProcessing as never;
      case "event":
        if (!this._event) this._event = new Smoker({ name: `consumerEvent` });
        return new Smoker({ name: this._event.name, source: this._event }) as never;
    }
  }
}

export namespace Consumer {
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, ERROR> = (value: VALUE, consumer: Consumer<VALUE, ERROR>) => void;
  export type Event<VALUE> =
    | EventShape<"ready" | "complete" | "drain">
    | EventShape<"abort" | "error", { error?: any }>;
  export type OnEvent<VALUE> = (event: Event<VALUE>) => void;
  export type Init<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    onEvent?: OnEvent<VALUE>;
    queue?: Queue<VALUE>;
    isReady?: boolean;
  };
}

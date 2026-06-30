import type { Closable, Evented, Named, NonEmptyString, Queue } from "./types";
import { LinkedList } from "./linked-list";
import { EventsLinker } from "./events-linker";

export class Consumer<VALUE, ERROR = any, NAME extends NonEmptyString = "consumer">
  implements Closable, Named<NAME>, Evented<EventsLinker.EventStreams<Consumer.Events<VALUE>, NAME>>
{
  readonly name: NAME;
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: Consumer.State;
  private _handler: Consumer.Handler<VALUE, ERROR, NAME>;
  private _ready: (self: Consumer<VALUE, ERROR, NAME>) => void;
  private _eventsLinker: EventsLinker<Consumer.Events<VALUE>, NAME, this>;

  constructor(handler: Consumer.Handler<VALUE, ERROR, NAME>, options?: Consumer.Options<VALUE, ERROR, NAME>) {
    const { events, isReady, queue } = options ?? {};

    this._eventsLinker = new EventsLinker(this, events);

    this._handler = handler;
    this._ready = events?.ready ?? (() => {});

    this.name = options?.name ?? ("consumer" as NAME);
    this._queue = queue ? queue : new LinkedList();
    this._isReady = isReady === undefined ? true : isReady;
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
        this._eventsLinker.emit("error", error);
      } finally {
        this._isProcessing = false;
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR): void {
    if (error) this._eventsLinker.emit("error", error);

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
          this._eventsLinker.emit("complete", undefined);
          this._clean();
        }
      } catch (error: any) {
        this._eventsLinker.emit("error", error);
      }
    } else {
      this._drain();
    }
  }
  abort(error?: ERROR): void {
    if (this._state === "aborted" || this._state === "completed") return;
    this._state = "aborted";

    try {
      this._eventsLinker.emit("abort", error);
    } catch (error: any) {
      this._eventsLinker.emit("error", error);
    } finally {
      this._clean();
    }
  }
  complete(): void {
    if (this._state !== "active") return;
    try {
      if (this._queue.size) {
        this._state = "drain";
        this.push = () => {};
        this._eventsLinker.emit("drain", undefined);
      } else {
        this._state = "completed";
        this._eventsLinker.emit("complete", undefined);
      }
    } catch (error: any) {
      this._eventsLinker.emit("error", error);
    } finally {
      this._clean();
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
      this._eventsLinker.emit("error", error);
    } finally {
      this._isProcessing = false;
    }
  }
  private _clean(): void {
    this._isReady = false;
    this._isProcessing = true;
    this._queue.clear();
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
  get handler(): Consumer.Handler<VALUE, ERROR, NAME> {
    return this._handler;
  }
  get events(): EventsLinker.EventStreams<Consumer.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
}

export namespace Consumer {
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any, any>;
  export type Handler<VALUE, ERROR, NAME extends NonEmptyString> = (
    self: Consumer<VALUE, ERROR, NAME>,
    value: VALUE,
  ) => void;

  export type Options<VALUE, ERROR, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    isReady?: boolean;
    events?: EventsLinker.EventsFunctions<Events<VALUE>, Consumer<VALUE, ERROR, NAME>>;
  };

  export type Events<VALUE> = {
    ready: void;
    enqueue: VALUE;
    dequeue: VALUE;
    drain: void;
    complete: void;
    abort: any;
    error: any;
  };
}

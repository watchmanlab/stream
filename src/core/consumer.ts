import type { Closable, Evented, EventsFunctions, EventStreams, Named, NonEmptyString, Queue, State } from "./types";
import { LinkedList } from "./linked-list";
import { EventsLinker } from "./events-linker";
import { InfosLinker } from "./infos-linker";
import { ScopeLinker } from "./scope-linker";

export class Consumer<VALUE, ERROR = any, NAME extends NonEmptyString = "consumer">
  implements Closable, Named<NAME>, Evented<Consumer.Events<VALUE>, NAME>
{
  readonly name: NAME;
  private _queue: Queue<VALUE>;
  private _isReady: boolean;
  private _isProcessing: boolean;
  private _state: State;
  private _handler: Consumer.Handler<VALUE, ERROR, NAME>;
  private _ready: (self: Consumer<VALUE, ERROR, NAME>) => void;
  private _eventsLinker: EventsLinker<Consumer.Events<VALUE>, NAME, this>;
  private _infosLinker: InfosLinker<Consumer.Infos<VALUE, ERROR, NAME>>;

  constructor(handler: Consumer.Handler<VALUE, ERROR, NAME>, options?: Consumer.Options<VALUE, ERROR, NAME>) {
    const { name, events, isReady, queue } = options ?? {};

    this._handler = handler;
    this._ready = events?.ready ?? (() => {});

    this.name = name ?? ("consumer" as NAME);
    this._queue = queue ? queue : new LinkedList();
    this._isReady = isReady === undefined ? true : isReady;
    this._isProcessing = false;
    this._state = "active";

    this._eventsLinker = new EventsLinker(this, events);
    this._infosLinker = new InfosLinker({
      handler: () => handler,
      isProcessing: () => this._isProcessing,
      isReady: () => this._isReady,
      queue: () => this._queue,
      state: () => this._state,
    });
  }

  push(value: VALUE): void {
    if (this._isReady && !this._isProcessing) {
      this._isProcessing = true;
      this._isReady = false;

      try {
        this._handler(this, value);
      } catch (error: any) {
        this._eventsLinker.emit("error", error);
      }
      this._isProcessing = false;
      if (this._isReady) this._ready(this);
    } else {
      this._queue.enqueue(value);
    }
  }
  next(error?: ERROR): void {
    if (error) this._eventsLinker.emit("error", error);

    if (this._isReady) {
      try {
        this._ready(this);
      } catch (error) {
        this._eventsLinker.emit("error", error);
      }
      return;
    }
    this._isReady = true;

    if (!this._isProcessing && this._queue.size === 0) {
      try {
        if (this._state === "active") {
          this._ready(this);
        } else {
          this._completed();
        }
      } catch (error: any) {
        this._eventsLinker.emit("error", error);
      }
    } else {
      this._drain();
    }
  }
  abort(error?: ERROR): void {
    this.push = this.next = this.complete = this.abort = this._drain = this._ready = this._handler = () => {};

    this._state = "aborted";
    this._isReady = true;
    this._isProcessing = false;
    this._queue.clear();

    if (error) this._eventsLinker.emit("error", error);

    try {
      this._eventsLinker.emit("abort", error);
      this._eventsLinker.abort(error);
    } catch (error: any) {
      this._eventsLinker.emit("error", error);
    }
  }
  complete(): void {
    this.push = this.complete = () => {};

    try {
      if (this._queue.size) {
        this._state = "drain";
        this._eventsLinker.emit("drain", undefined);
      } else {
        this._completed();
      }
    } catch (error: any) {
      this._eventsLinker.emit("error", error);
    }
  }
  private _drain(): void {
    if (this._isProcessing) return;

    this._isProcessing = true;
    while (this._isReady && this._queue.size) {
      this._isReady = false;

      const value = this._queue.dequeue() as VALUE;

      try {
        this._handler(this, value);
      } catch (error: any) {
        this._eventsLinker.emit("error", error);
      }
    }
    this._isProcessing = false;
  }
  private _completed(): void {
    this.next = this.abort = this._ready = this._handler = () => {};
    this._state = "completed";
    this._isReady = true;
    this._isProcessing = false;
    this._eventsLinker.emit("complete", undefined);
    this._eventsLinker.complete();
  }

  get infos(): Consumer.Infos<VALUE, ERROR, NAME> {
    return this._infosLinker.infos;
  }
  get events(): EventStreams<Consumer.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any, any, any>;
  export type Handler<VALUE, ERROR, NAME extends NonEmptyString> = (
    self: Consumer<VALUE, ERROR, NAME>,
    value: VALUE,
  ) => void;

  export type Options<VALUE, ERROR, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    isReady?: boolean;
    events?: EventsFunctions<Events<VALUE>, Consumer<VALUE, ERROR, NAME>>;
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
  export type Infos<VALUE, ERROR, NAME extends NonEmptyString> = {
    state: State;
    queue: Queue<VALUE>;
    isReady: boolean;
    isProcessing: boolean;
    handler: Handler<VALUE, ERROR, NAME>;
  };
}

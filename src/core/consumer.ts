import type { Closable, Evented, EventHandlers, EventStreams, Named, NonEmptyString, Queue, State } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { EventsLinker } from "./events-linker";
import { InfosLinker } from "./infos-linker";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer">
  implements Closable, Named<NAME>, Evented<Consumer.Events<VALUE>, NAME>
{
  readonly name: NAME;
  private _queue: Queue<VALUE>;
  private _ready: boolean;
  private _processing: boolean;
  private _state: State;
  private _handler: Consumer.Handler<VALUE, NAME>;
  private _next: (self: Consumer<VALUE, NAME>) => void;
  private _eventsLinker: EventsLinker<Consumer.Events<VALUE>, NAME, this>;
  private _infosLinker: InfosLinker<Consumer.Infos<VALUE, NAME>>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    const { name, events, ready, queue } = options ?? {};

    this._handler = handler;
    this._next = events?.next ?? (() => {});

    this.name = name ?? ("consumer" as NAME);
    this._queue = queue ? queue : new LinkedListQueue();
    this._ready = ready === undefined ? true : ready;
    this._processing = false;
    this._state = "active";

    this._eventsLinker = new EventsLinker(this, events);
    this._infosLinker = new InfosLinker({
      handler: () => handler,
      processing: () => this._processing,
      ready: () => this._ready,
      queue: () => this._queue,
      state: () => this._state,
    });
  }

  push(value: VALUE): void {
    if (this._ready && !this._processing) {
      this._processing = true;
      this._ready = false;

      this._handler(this, value);
      // try {
      // } catch (error: any) {
      //   this._eventsLinker.emit("error", error);
      // }
      this._processing = false;
      if (this._ready) {
        this._next(this);
        // try {
        // } catch (error: any) {
        //   this._eventsLinker.emit("error", error);
        // }
      }
    } else {
      this._queue.enqueue(value);
    }
  }
  next(): void {
    if (this._ready) return this._next(this);

    this._ready = true;

    if (!this._processing && this._queue.size === 0) {
      if (this._state === "active") {
        this._next(this);
      } else {
        this._completed();
      }
    } else {
      this._drain();
    }
  }
  abort(): void {
    this.push = this.next = this.complete = this.abort = this._drain = this._next = this._handler = () => {};

    this._state = "aborted";
    this._ready = true;
    this._processing = false;
    this._queue.clear();

    this._eventsLinker.emit("abort", undefined);
    this._eventsLinker.abort();
  }
  complete(): void {
    this.push = this.complete = () => {};

    if (this._queue.size) {
      this._state = "drain";
      this._eventsLinker.emit("drain", undefined);
    } else {
      this._completed();
    }
  }
  private _drain(): void {
    if (this._processing) return;

    this._processing = true;
    while (this._ready && this._queue.size) {
      this._ready = false;

      const value = this._queue.dequeue() as VALUE;

      this._handler(this, value);
    }
    this._processing = false;
  }
  private _completed(): void {
    this.next = this.abort = this._next = this._handler = () => {};
    this._state = "completed";
    this._ready = true;
    this._processing = false;

    this._eventsLinker.emit("complete", undefined);
    this._eventsLinker.complete();
  }
  get infos(): Consumer.Infos<VALUE, NAME> {
    return this._infosLinker.infos;
  }
  get events(): EventStreams<Consumer.Events<VALUE>, NAME> {
    return this._eventsLinker.events;
  }
}

export namespace Consumer {
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (self: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    events?: EventHandlers<Events<VALUE>, Consumer<VALUE, NAME>>;
  };

  export type Events<VALUE> = {
    next: void;
    enqueue: VALUE;
    dequeue: VALUE;
    drain: void;
    complete: void;
    abort: void;
  };
  export type Infos<VALUE, NAME extends NonEmptyString> = {
    state: State;
    queue: Queue<VALUE>;
    ready: boolean;
    processing: boolean;
    handler: Handler<VALUE, NAME>;
  };
}

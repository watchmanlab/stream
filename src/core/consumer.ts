import type { Closable, Named, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Named<NAME>, Closable, Named<NAME> {
  private _name: NAME;
  private _state: Consumer.State;
  private _queue: Queue<VALUE>;
  private _ready: boolean;
  private _processing: boolean;

  private _handler: Consumer.Handler<VALUE, NAME>;
  private _next: NonNullable<Consumer.Options<VALUE, NAME>["next"]>;
  private _drain: NonNullable<Consumer.Options<VALUE, NAME>["drain"]>;
  private _terminate: NonNullable<Consumer.Options<VALUE, NAME>["terminate"]>;
  private _$terminate?: Stream<"abort" | "complete", `$${NAME}Terminate`>;
  private _$drain?: Stream<void, `$${NAME}Drain`>;
  private _$next?: Stream<void, `$${NAME}Next`>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    this._name = options?.name ?? ("consumer" as NAME);
    this._state = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._ready = options?.ready ?? true;
    this._processing = false;

    this._handler = handler;
    this._next = options?.next ?? (() => {});
    this._drain = options?.drain ?? (() => {});
    this._terminate = options?.terminate ?? (() => {});
  }

  get name() {
    return this._name;
  }
  get state() {
    return this._state;
  }
  get queue() {
    return this._queue;
  }
  get $drain() {
    return (this._$drain ??= new Stream({ name: `$${this.name}Drain` }));
  }
  get $terminate() {
    return (this._$terminate ??= new Stream({ name: `$${this.name}Terminate` }));
  }
  get $next() {
    return (this._$next ??= new Stream({ name: `$${this.name}Next` }));
  }

  push(value: VALUE): void {
    const { _queue, _handler } = this;
    if (this._ready && !this._processing) {
      this._processing = true;
      this._ready = false;
      _handler(this, value);
      this._processing = false;
      if (this._ready) this.next();
    } else {
      _queue.enqueue(value);
    }
  }

  next(): void {
    const { _queue, _handler, _$next } = this;
    if (this._ready && !this._queue.size) {
      this._next(this);
      _$next?.push();
      return;
    }

    this._ready = true;
    if (this._processing) return;

    this._processing = true;
    while (this._ready && _queue.size) {
      this._ready = false;
      const value = _queue.dequeue() as VALUE;
      _handler(this, value);
    }

    switch (this._state) {
      case "active":
        this._next(this);
        _$next?.push();
        break;
      case "draining":
        if (!_queue.size) this.terminate("complete");
        break;
    }

    this._processing = false;
  }

  terminate(reason: "abort" | "complete"): void {
    const { _queue, _$next, _$drain, _$terminate } = this;
    this.push = () => {};
    if (reason === "abort") {
      this.next = this.terminate = () => {};
      this._state = "aborted";
      _queue.clear();
    } else if (this._queue.size) {
      this._state = "draining";
      this._drain(this);
      _$drain?.push();
      return;
    } else {
      this.next = this.terminate = () => {};
      this._state = "completed";
    }

    _$terminate?.push(reason);
    _$terminate?.terminate("complete");
    _$drain?.terminate("complete");
    _$next?.terminate("complete");

    this._$terminate = this._$drain = this._$next = undefined;

    this._terminate(this, reason);
    this._handler = this._next = this._drain = this._terminate = () => {};
  }
}

export namespace Consumer {
  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    drain?: (consumer: Consumer<VALUE, NAME>) => void;
    terminate?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
  };
}

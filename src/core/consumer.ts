import { Closable, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Closable {
  private _name: NAME;
  private _state: Consumer.State;
  private _queue: Queue<VALUE>;
  private _counter: number;

  private _handler: Consumer.Handler<VALUE, NAME>;
  private _next: NonNullable<Consumer.Options<VALUE, NAME>["next"]>;
  private _drain: NonNullable<Consumer.Options<VALUE, NAME>["drain"]>;
  private _terminate: NonNullable<Consumer.Options<VALUE, NAME>["terminate"]>;
  private _$terminate?: Stream<"abort" | "complete", `${NAME}Terminate`>;
  private _$drain?: Stream<void, `${NAME}Drain`>;
  private _$next?: Stream<void, `${NAME}Next`>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    this._name = options?.name ?? ("consumer" as NAME);
    this._state = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._counter = 0;

    this._handler = handler;
    this._next = options?.next ?? (() => {});
    this._drain = options?.drain ?? (() => {});
    this._terminate = options?.terminate ?? (() => {});
  }

  get state() {
    return this._state;
  }
  get queue() {
    return this._queue;
  }
  get $next() {
    return (this._$next ??= new Stream({
      name: `${this._name}Next`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$next = undefined;
      },
    }));
  }
  get $drain() {
    return (this._$drain ??= new Stream({
      name: `${this._name}Drain`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$drain = undefined;
      },
    }));
  }
  get $terminate() {
    return (this._$terminate ??= new Stream({
      name: `${this._name}Terminate`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._$terminate = undefined;
      },
    }));
  }

  push(value: VALUE) {
    const { _queue } = this;
    if (this._counter > 0 && _queue.size === 0) {
      this._handler(this, value);
      this._counter--;
    } else {
      _queue.enqueue(value);
    }
  }

  next() {
    const { _queue } = this;
    this._counter++;
    if (_queue.size === 0) {
      this._next(this);
      this._$next?.push();
      return this;
    }

    if (this._counter > 1) return this;

    while (this._counter > 0) {
      const value = _queue.dequeue();
      if (value === Queue.EMPTY) {
        if (this._state === "draining") {
          this.terminate("complete");
        } else {
          this._next(this);
          this._$next?.push();
        }
        break;
      }

      this._handler(this, value);
      this._counter--;
    }
  }

  terminate(reason: "abort" | "complete") {
    const { _queue, _$next, _$drain, _$terminate } = this;
    this.push = () => this;
    if (reason === "abort") {
      this.next = this.terminate = () => this;
      this._state = "aborted";
      _queue.clear();
    } else if (this._queue.size) {
      this._state = "draining";
      this._drain(this);
      _$drain?.push();
      return this;
    } else {
      this.next = this.terminate = () => this;
      this._state = "completed";
    }

    _$terminate?.push(reason);
    _$terminate?.terminate("complete");
    _$drain?.terminate("complete");
    _$next?.terminate("complete");

    this._$terminate = this._$drain = this._$next = undefined;

    this._terminate(this, reason);
    this._handler = this._next = this._drain = this._terminate = () => this;
  }
}

export namespace Consumer {
  export type State = "active" | "draining" | "aborted" | "completed";

  export type Handler<VALUE, NAME extends NonEmptyString> = (self: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    next?: (self: Consumer<VALUE, NAME>) => void;
    drain?: (self: Consumer<VALUE, NAME>) => void;
    terminate?: (self: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
  };
}

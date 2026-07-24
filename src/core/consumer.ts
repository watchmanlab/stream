import { Closable, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Closable<NAME> {
  protected _options: Consumer.Options<VALUE, NAME>;
  private _state: Consumer.State;
  private _queue: Queue<VALUE>;
  private _counter: number;

  private _handler: Consumer.Handler<VALUE, NAME>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    this._options = { ...options };
    this._state = "active";
    this._queue = options?.queue ?? new LinkedListQueue();
    this._counter = 0;

    this._handler = handler;
  }
  get name() {
    return this._options.name ?? ("consumer" as NAME);
  }
  get state() {
    return this._state;
  }
  get queue() {
    return this._queue;
  }
  get $next() {
    return (this._options.$next ??= new Stream({
      name: `${this.name}Next`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$next = undefined;
      },
    }));
  }
  get $drain() {
    return (this._options.$drain ??= new Stream({
      name: `${this.name}Drain`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$drain = undefined;
      },
    }));
  }
  get $terminate() {
    return (this._options.$terminate ??= new Stream({
      name: `${this.name}Terminate`,
      consumerLeft: (self) => {
        if (!self.consumers.count) this._options.$terminate = undefined;
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

  next(): void {
    this._counter++;
    const { _queue } = this;

    if (_queue.size === 0) {
      this._options.next?.(this);
      this._options.$next?.push();
      return;
    }

    if (this._counter > 1) return;

    while (this._counter > 0) {
      const value = _queue.dequeue();
      if (value === Queue.EMPTY) {
        if (this._state === "draining") {
          this.terminate("complete");
        } else {
          this._options.next?.(this);
          this._options.$next?.push();
        }
        break;
      }

      this._handler(this, value);
      this._counter--;
    }
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => this;
    if (reason === "abort") {
      this.next = this.terminate = () => {};
      this._state = "aborted";
      this._queue.clear();
    } else if (this._queue.size) {
      this._state = "draining";
      this._options.drain?.(this);
      this._options.$drain?.push();
      return;
    } else {
      this.next = this.terminate = () => {};
      this._state = "completed";
    }

    this._options.$next?.terminate(reason);
    this._options.$drain?.terminate(reason);

    this._options.terminate?.(this, reason);
    this._options.$terminate?.push(reason);
    this._options.$terminate?.terminate(reason);
    this._handler = () => {};
    this._options = {};
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
    $next?: Stream<void, `${NAME}Next`>;
    $drain?: Stream<void, `${NAME}Drain`>;
    $terminate?: Stream<"abort" | "complete", `${NAME}Terminate`>;
  };
}

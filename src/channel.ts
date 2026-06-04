import { Queue } from "./queue";
import { Stream } from "./stream";

export class Channel<VALUE> {
  private _queue = new Queue<VALUE>();
  private _status: Channel.Status = "active";
  private _pending?: { promise: Promise<VALUE | Channel.Done>; resolver: (value: VALUE | Channel.Done) => void };
  private _completed?: Stream<Channel.Done, "completed">;
  private _aborted?: Stream<Channel.Done, "aborted">;
  private _stream?: Stream<VALUE, "channel">;

  constructor(private options?: Channel.Options<Channel<VALUE>>) {}

  push(value: VALUE) {
    if (this._status !== "active") return;
    if (this._pending) {
      this._pending.resolver(value);
      this._pending = undefined;
    } else {
      this._queue.enqueue(value);
    }
  }

  static handleNext<VALUE, CHANNEL extends Channel<VALUE>>(
    channel: CHANNEL,
    next: VALUE | Channel.Done | Promise<VALUE | Channel.Done>,
    handlers: Channel.NextHandlers<VALUE, CHANNEL>,
  ) {
    if (next instanceof Promise) {
      next.then((value) => {
        if (value !== Channel.DONE) {
          handlers.onValue?.(value, channel);
          return;
        }

        if (channel.status === "completed") {
          handlers.onComplete?.(channel);
        } else {
          handlers.onAbort?.(channel);
        }
        handlers.onDone?.(channel);
      });
    } else {
      if (next !== Channel.DONE) {
        handlers.onValue?.(next, channel);
        return;
      }

      if (channel.status === "completed") {
        handlers.onComplete?.(channel);
      } else {
        handlers.onAbort?.(channel);
      }
      handlers.onDone?.(channel);
    }
  }
  handleNext(handlers: Channel.NextHandlers<VALUE, this>) {
    Channel.handleNext(this, this.next(), handlers);
  }
  next(): VALUE | Channel.Done | Promise<VALUE | Channel.Done> {
    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) return value;

    if (this._status === "drain") {
      this.complete();
      return Channel.DONE;
    } else if (this._status === "aborted" || this._status === "completed") {
      return Channel.DONE;
    }

    if (this._pending) return this._pending.promise;

    let resolver!: (value: VALUE | Channel.Done) => void;

    const promise = new Promise<VALUE | Channel.Done>((r) => {
      resolver = r;
      this.options?.pull?.(this);
    });

    this._pending = { promise, resolver };
    return promise;
  }
  complete() {
    if (this._queue.size) {
      this._status = "drain";
    } else {
      this._status = "completed";
      this.options?.complete?.(this);
      this.options?.done?.(this);
      this._completed?.abort();
      this.options = this._aborted = this._completed = this._stream = undefined;
    }
  }
  abort() {
    this._status = "aborted";
    this._queue.clear();
    this._pending?.resolver(Channel.DONE);
    this.options?.abort?.(this);
    this.options?.done?.(this);
    this._aborted?.abort();
    this._pending = this.options = this._aborted = this._completed = this._stream = undefined;
  }
  get stream() {
    if (!this._stream) this._stream = new Stream({ name: "channel", source: this, scoop: this });
    return this._stream;
  }
  get status() {
    return this._status;
  }
  get queue() {
    return this._queue;
  }
  get hasPending() {
    return this._pending !== undefined;
  }
  get completed() {
    if (!this._completed) this._completed = new Stream({ name: "completed" });
    return this._completed;
  }
  get aborted() {
    if (!this._aborted) this._aborted = new Stream({ name: "aborted" });
    return this._aborted;
  }
}

export namespace Channel {
  export type AnyChannel = Channel<any>;
  export type Status = "active" | "drain" | "completed" | "aborted";
  export type Options<SELF extends AnyChannel> = {
    pull?: (self: SELF) => void;
    done?: (self: SELF) => void;
    complete?: (self: SELF) => void;
    abort?: (self: SELF) => void;
  };

  export type NextHandlers<VALUE, SELF extends Channel<VALUE>> =
    | {
        onValue: (value: VALUE, self: SELF) => void;
        onAbort?: (self: SELF) => void;
        onComplete?: (self: SELF) => void;
        onDone?: (self: SELF) => void;
      }
    | {
        onValue?: (value: VALUE, self: SELF) => void;
        onAbort: (self: SELF) => void;
        onComplete?: (self: SELF) => void;
        onDone?: (self: SELF) => void;
      }
    | {
        onValue?: (value: VALUE, self: SELF) => void;
        onAbort?: (self: SELF) => void;
        onComplete: (self: SELF) => void;
        onDone?: (self: SELF) => void;
      }
    | {
        onValue?: (value: VALUE, self: SELF) => void;
        onAbort?: (self: SELF) => void;
        onComplete?: (self: SELF) => void;
        onDone: (self: SELF) => void;
      };
  export const DONE = Symbol.for("done");
  export type Done = typeof DONE;
}

import { Queue } from "./queue";
import { Stream } from "./stream";

export class Channel<VALUE> {
  private _queue = new Queue<VALUE>();
  private _status: Channel.Status = "active";
  private _pending?: {
    promise: Promise<VALUE>;
    resolve: (value: VALUE) => void;
    reject: (reason: Channel.Aborted | Channel.Completed) => void;
  };
  private _terminated?: Stream<Channel.Aborted | Channel.Completed, "terminated">;
  private _stream?: Stream<VALUE, "channel">;

  constructor(private options?: Channel.Options<Channel<VALUE>>) {}

  push(value: VALUE) {
    if (this._status !== "active") return;
    if (this._pending) {
      this._pending.resolve(value);
      this._pending = undefined;
    } else {
      this._queue.enqueue(value);
    }
  }

  once(handlers: Channel.OnceHandlers<VALUE, this>): this {
    try {
      const next = this.next();
      if (next instanceof Promise) {
        next
          .then((next) => handlers.next?.(next, this))
          .catch((reason) => {
            if (reason === Channel.COMPLETED) {
              handlers.complete?.(this);
              handlers.terminate?.(Channel.COMPLETED, this);
            } else if (reason === Channel.ABORTED) {
              handlers.abort?.(this);
              handlers.terminate?.(Channel.ABORTED, this);
            } else {
              handlers.error?.(reason, this);
            }
          });
      } else {
        handlers.next?.(next, this);
      }
    } catch (reason) {
      if (reason === Channel.COMPLETED) {
        handlers.complete?.(this);
        handlers.terminate?.(Channel.COMPLETED, this);
      } else if (reason === Channel.ABORTED) {
        handlers.abort?.(this);
        handlers.terminate?.(Channel.ABORTED, this);
      } else {
        handlers.error?.(reason, this);
      }
    }
    return this;
  }
  next(): VALUE | Promise<VALUE> {
    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) return value;

    switch (this._status) {
      case "completed":
        throw Channel.COMPLETED;
      case "aborted":
        throw Channel.ABORTED;
      case "drain":
        this.terminate(Channel.COMPLETED);
        throw Channel.COMPLETED;
    }

    if (this._pending) return this._pending.promise;

    let resolve!: (value: VALUE) => void;
    let reject!: (reason: Channel.Aborted | Channel.Completed) => void;

    const promise = new Promise<VALUE>((res, rej) => {
      resolve = res;
      reject = rej;
      this.options?.pull?.(this);
    });

    this._pending = { promise, resolve, reject };
    return promise;
  }
  terminate(reason: Channel.Aborted | Channel.Completed) {
    if (this._status == "aborted" || this._status === "completed") return;
    if (reason === Channel.COMPLETED) {
      if (this._queue.size) {
        this._status = "drain";
        return;
      } else {
        this._status = "completed";
        this._pending?.reject(Channel.COMPLETED);
        this._terminated?.push(Channel.COMPLETED);
      }
    } else {
      this._status = "aborted";
      this._queue.clear();
      this._pending?.reject(Channel.ABORTED);
      this._terminated?.push(Channel.ABORTED);
    }
    this.options?.terminate?.(reason, this);
    this._terminated?.terminate(reason);
    this.options = this._terminated = this._stream = this._pending = undefined;
  }

  get stream() {
    if (!this._stream) this._stream = new Stream({ name: "channel", source: this, scope: this });
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
  get terminated() {
    if (!this._terminated) this._terminated = new Stream({ name: "terminated" });
    return this._terminated;
  }
}

export namespace Channel {
  export type AnyChannel = Channel<any>;
  export type Status = "active" | "drain" | "completed" | "aborted";
  export type Options<SELF extends AnyChannel> = {
    pull?: (self: SELF) => void;
    terminate?: (reason: Aborted | Completed, self: SELF) => void;
  };

  export type OnceHandlers<VALUE, SELF extends Channel<VALUE>> = {
    next?: (value: VALUE, self: SELF) => void;
    complete?: (self: SELF) => void;
    abort?: (self: SELF) => void;
    terminate?: (reason: Aborted | Completed, self: SELF) => void;
    error?: (reason: any, self: SELF) => void;
  };
  export const COMPLETED = Symbol.for("completed");
  export type Completed = typeof COMPLETED;
  export const ABORTED = Symbol.for("aborted");
  export type Aborted = typeof ABORTED;
}

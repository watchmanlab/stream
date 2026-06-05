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
  private _closed?: Stream<boolean, "closed">;
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

  next(): VALUE | Promise<VALUE> {
    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) return value;

    switch (this._status) {
      case "completed":
        throw Channel.COMPLETED;
      case "aborted":
        throw Channel.ABORTED;
      case "drain":
        this.close(true);
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
  close(complete = false) {
    if (this._status == "aborted" || this._status === "completed") return;
    if (complete) {
      if (this._queue.size) {
        this._status = "drain";
        return;
      } else {
        this._status = "completed";
        this._pending?.reject(Channel.COMPLETED);
        this._closed?.push(true);
      }
    } else {
      this._status = "aborted";
      this._queue.clear();
      this._pending?.reject(Channel.ABORTED);
      this._closed?.push(false);
    }
    this.options?.close?.(complete, this);
    this._closed?.close(true);
    this.options = this._closed = this._stream = this._pending = undefined;
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
  get closed() {
    if (!this._closed) this._closed = new Stream({ name: "closed" });
    return this._closed;
  }
}

export namespace Channel {
  export type AnyChannel = Channel<any>;
  export type Status = "active" | "drain" | "completed" | "aborted";
  export type Options<SELF extends AnyChannel> = {
    pull?: (self: SELF) => void;
    close?: (complete: boolean, self: SELF) => void;
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

  export const COMPLETED = Symbol.for("completed");
  export type Completed = typeof COMPLETED;
  export const ABORTED = Symbol.for("aborted");
  export type Aborted = typeof ABORTED;
}

import { Queue } from "./queue";

export class Channel<VALUE> {
  private _queue = new Queue<VALUE>();
  private _status: Channel.Status = "active";
  private _pending?: { promise: Promise<VALUE | Channel.Done>; resolver: (value: VALUE | Channel.Done) => void };

  constructor(private options?: Channel.Options) {}

  push(value: VALUE) {
    if (this._status !== "active") return;
    if (this._pending) {
      this._pending.resolver(value);
      this._pending = undefined;
    } else {
      this._queue.enqueue(value);
    }
  }
  next() {
    if (this._status === "aborted") return Channel.DONE;

    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) return value;

    if (this._status === "drain") return Channel.DONE;

    if (this._pending) return this._pending.promise;

    const pending: any = {};

    pending.promise = new Promise<VALUE | Channel.Done>((r) => {
      pending.resolver = r;
      this.options?.pull?.();
    });

    this._pending = pending;

    return this._pending!.promise;
  }

  complete() {
    this._status = "drain";
    this.options?.done?.();
  }
  abort() {
    this._status = "aborted";
    this._queue.clear();
    this._pending?.resolver(Channel.DONE);
    this._pending = undefined;
    this.options?.done?.();
  }
  get status() {
    return this._status;
  }
  get queue() {
    return this._queue;
  }
  get pending() {
    return this._pending !== undefined;
  }
}

export namespace Channel {
  export type Status = "active" | "drain" | "aborted";
  export type Options = {
    pull?: () => void;
    done?: () => void;
  };
  export const DONE = Symbol.for("done");
  export type Done = typeof DONE;
}

function test() {
  const MAX = 1_000_000;
  const start = performance.now();

  const stream = new Channel<number>();

  (async () => {
    let next = stream.next();
    next = next instanceof Promise ? await next : next;

    while (next !== Channel.DONE) {
      if (next === MAX) console.log(next, Math.round(performance.now() - start));
      next = stream.next();
      next = next instanceof Promise ? await next : next;
    }
  })();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
  //   function* gen() {
  //     for (let i = 0; i <= MAX; i++) {
  //       yield i;
  //     }
  //   }
  //   (async () => {
  //     for (const value of gen()) {
  //       if (value === MAX) console.log(value, Math.round(performance.now() - start));
  //     }
  //   })();
}

test();

import { LinkedList } from "./linked-list";

export class Channel<VALUE> {
  private _values = new LinkedList<VALUE>();
  private _handlers = new LinkedList<Channel.Listener<VALUE>>();

  push(value: VALUE) {
    if (this._handlers.size) {
      const handler = this._handlers.dequeue() as Channel.Listener<VALUE>;
      handler(value, this, handler);
    } else {
      this._values.enqueue(value);
    }
  }

  next(handler: Channel.Listener<VALUE>) {
    if (this._values.size) {
      const value = this._values.dequeue() as VALUE;
      handler(value, this, handler);
    } else if (this.push !== null) {
      this._handlers.enqueue(handler);
    }
  }

  abort() {
    this._values.clear();
    this._handlers.clear();
    this.push = null!;
    this.next = null!;
  }
  complete() {
    this.push = null!;
  }
  get state(): Channel.State {
    if (this.next === null) return "aborted";
    if (this.push === null) {
      return this._values.size > 0 ? "draining" : "completed";
    }
    return "active";
  }
  get isReady() {
    return this._handlers.size > 0;
  }
  get values() {
    return this._values;
  }
  get handlers() {
    return this._handlers;
  }
}

export namespace Channel {
  export type State = "active" | "draining" | "completed" | "aborted";
  export type Listener<VALUE> = (value: VALUE, self: Channel<VALUE>, handler: Listener<VALUE>) => void;
}

function test() {
  const MAX = 100_000;
  const channel = new Channel<number>();
  const start = performance.now();

  for (let i = 0; i <= MAX; i++) {
    channel.push(i);
  }

  channel.next((value, self, handler) => {
    if (value === MAX) {
      console.log(value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      //   channel.abort();
    }
    self.next(handler);
  });
}

test(); // 100 000 000 744 ms

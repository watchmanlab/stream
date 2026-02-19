import { Stream } from "../../stream/stream-0";
const NAME = "queued";
type Name = typeof NAME;
export class Queue<VALUE> extends Stream<VALUE, Name> {
  protected _buffer = new Array<VALUE>();
  protected _events?: Stream<Queue.Event<VALUE>, "Event">;
  protected _options: Required<Queue.Options> = { dropStrategy: "oldest", maxSize: 10000 };
  protected _dropped = 0;
  protected _resolvers = new Set<() => void>();
  constructor(options?: Queue.Options) {
    super(NAME);

    this.options = options ?? {};
  }

  override async push(value: VALUE, ...values: VALUE[]): Promise<void> {
    values.unshift(value);
    for (let i = 0; i < values.length; i++) {
      const value = values[i];
      if (this._buffer.length >= this._options.maxSize) {
        this._dropped++;
        if (this._options.dropStrategy === "newest") {
          this._events?.push({ type: "evicted", value, self: this });
          continue;
        } else {
          this._events?.push({ type: "evicted", value: this._buffer.pop()!, self: this });
        }
      }

      this._buffer.unshift(value);
      this._events?.push({ type: "buffered", value, self: this });
      this._resolvers.forEach((resolver) => resolver());
      this._resolvers.clear();
    }

    await new Promise((r) => setTimeout(r));
  }
  override async *[Symbol.asyncIterator]() {
    let resolve;

    try {
      while (true) {
        if (this._buffer.length) {
          const value = this._buffer.pop()!;
          yield value;
          this._events?.push({ type: "consumed", value, self: this });
        } else {
          await new Promise<void>((res) => {
            resolve = res;
            this._resolvers.add(resolve);
          });
        }
      }
    } finally {
      this._resolvers.delete(resolve!);
      resolve!();
    }
  }
  get events() {
    if (!this._events) this._events = new Stream();
    return this._events;
  }
  get options() {
    return { ...this._options };
  }
  set options(options: Queue.Options) {
    this._options = { ...this._options, ...options };
  }
  get dropped() {
    return this._dropped;
  }
  get size() {
    return this._buffer.length;
  }
  get values() {
    return [...this._buffer];
  }
  clear() {
    this._buffer.length = 0;
  }
}

export namespace Queue {
  export type Options = {
    maxSize?: number;
    dropStrategy?: "oldest" | "newest";
  };

  export type Event<VALUE> =
    | { type: "evicted"; value: VALUE; self: Queue<VALUE> }
    | { type: "buffered"; value: VALUE; self: Queue<VALUE> }
    | { type: "consumed"; value: VALUE; self: Queue<VALUE> };
}

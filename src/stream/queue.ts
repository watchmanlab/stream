import { Stream } from "./stream";

export class Queue<VALUE, NAME extends string> implements Iterable<VALUE> {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;
  private _options: Required<Queue.Options>;
  private _valueQueued?: Stream<VALUE, never, `${NAME}ValueQueued`>;
  private _valueDropped?: Stream<VALUE, never, `${NAME}ValueDropped`>;
  constructor(
    public readonly name: NAME,
    options?: Queue.Options,
  ) {
    this._options = { ...Queue.defaultOptions, ...options };
  }
  get valueQueued() {
    if (!this._valueQueued) this._valueQueued = new Stream(`${this.name}ValueQueued`);
    return this._valueQueued;
  }
  get valueDropped() {
    if (!this._valueDropped) this._valueDropped = new Stream(`${this.name}ValueDropped`);
    return this._valueDropped;
  }
  get options() {
    return this._options;
  }
  set options(options: Queue.Options) {
    this._options = {
      ...this._options,
      ...Object.fromEntries(Object.entries(options).filter(([_, val]) => val != null)),
    };
  }
  get size() {
    return this._size;
  }
  enqueue(value: VALUE): Queue.EnqueueResult<VALUE> {
    if (this.size >= this._options.maxSize && this._options.dropStrategy === "newest") {
      this._valueDropped?.push(value);
      return { ok: false, dropped: value };
    }
    this._size++;
    const node = { value };
    if (!this._head) {
      this._head = this._tail = node;
    } else {
      this._tail!.next = node;
      this._tail = node;
    }
    if (this.size > this._options.maxSize && this._options.dropStrategy === "oldest") {
      const dropped = this.dequeue() as VALUE;
      this._valueDropped?.push(dropped);
      return { ok: false, dropped };
    }
    this._valueQueued?.push(value);
    return { ok: true };
  }
  dequeue(): VALUE | Queue.Empty {
    if (!this._head) return Queue.EMPTY;
    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    return value;
  }
  clear() {
    for (const value of this) {
      this._valueDropped?.push(value);
    }
  }
  [Symbol.iterator]() {
    const self = this;
    return {
      next: () => {
        const value = self.dequeue();
        return { value: value as VALUE, done: value === Queue.EMPTY };
      },
    };
  }
}
export namespace Queue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;
  export type DropStrategy = "newest" | "oldest";
  export type Options = {
    maxSize?: number;
    dropStrategy?: DropStrategy;
  };
  export const defaultOptions: Required<Options> = {
    maxSize: Number.MAX_SAFE_INTEGER,
    dropStrategy: "newest",
  };
  export type EnqueueResult<VALUE> = { ok: true; dropped?: never } | { ok: false; dropped: VALUE };
  export const EMPTY = Symbol("$EMPTY#");
  export type Empty = typeof EMPTY;
}

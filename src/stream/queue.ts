import { Stream } from "./stream";

export class Queue<VALUE, NAME extends string> implements Iterable<VALUE> {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;
  private _options: Required<Queue.Options>;
  private _valueDropped?: Stream<VALUE, never, `${NAME}ValueDropped`>;
  constructor(
    public readonly name: NAME,
    options?: Queue.Options,
  ) {
    this._options = { ...Queue.defaultOptions, ...options };
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
  enqueue(value: VALUE): boolean {
    if (this.size >= this._options.maxSize && this._options.dropStrategy === "newest") {
      this._valueDropped?.push(value);
      return false;
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
      this._valueDropped?.push(this.dequeue() as VALUE);
      return false;
    }
    return true;
  }
  dequeue(): VALUE | Queue.Empty {
    if (!this._head) return Queue.EMPTY;
    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    return value;
  }
  clear() {
    this._head = this._tail = undefined;
    this._size = 0;
  }
  [Symbol.iterator]() {
    const self = this;
    return {
      next: () => {
        if (self._head) {
          const value = self._head.value;
          self._head = self._head.next;
          return { value };
        } else {
          return { value: Queue.EMPTY as never, done: true };
        }
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
  export const EMPTY = Symbol("$EMPTY#");
  export type Empty = typeof EMPTY;
}

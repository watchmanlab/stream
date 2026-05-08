import { Stream } from "./stream.ts";

export class Queue<VALUE, NAME extends string> implements Iterable<Stream.Batch<VALUE>>, AsyncDisposable, Disposable {
  readonly name: NAME;
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;
  private _options: Required<Queue.Options>;
  private _valueQueued?: Stream<VALUE, never, `${NAME}ValueQueued`>;
  private _valueDropped?: Stream<VALUE, never, `${NAME}ValueDropped`>;
  private _cleared?: Stream<void, never, `${NAME}Cleared`>;
  private _disposed?: Stream<void, never, `${NAME}Disposed`>;

  constructor(name: NAME, options?: Queue.Options) {
    this.name = name;
    this._options = { ...Queue.defaultOptions, ...options };
  }
  [Symbol.iterator]() {
    const self = this;
    return {
      next: () => {
        const value = self.dequeue();
        return { value: value as Stream.Batch<VALUE>, done: value === Queue.EMPTY };
      },
    };
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  enqueue(value: Stream.Batch<VALUE>): Queue.EnqueueResult<VALUE> {
    if (this.size >= this._options.maxSize && this._options.dropStrategy === "newest") {
      this._valueDropped?.pushMany(value);
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
      const dropped = this.dequeue() as Stream.Batch<VALUE>;
      this._valueDropped?.pushMany(dropped);
      return { ok: false, dropped };
    }
    this._valueQueued?.pushMany(value);
    return { ok: true };
  }
  dequeue(): Stream.Batch<VALUE> | Queue.Empty {
    if (!this._head) return Queue.EMPTY;
    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    return value;
  }
  clear() {
    for (const value of this) {
      this._valueDropped?.pushMany(value);
    }
    this._cleared?.push();
  }
  async dispose() {
    this.clear();
    await Promise.all([this._valueQueued?.dispose(), this._valueDropped?.dispose(), this._cleared?.dispose()]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this._valueQueued = this._valueDropped = this._cleared = this._disposed = undefined;
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
  get valueQueued() {
    if (!this._valueQueued) this._valueQueued = new Stream(`${this.name}ValueQueued`);
    return this._valueQueued;
  }
  get valueDropped() {
    if (!this._valueDropped) this._valueDropped = new Stream(`${this.name}ValueDropped`);
    return this._valueDropped;
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Stream(`${this.name}Cleared`);
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`${this.name}Disposed`);
    return this._disposed;
  }
}
export namespace Queue {
  export type AnyQueue = Queue<any, any>;
  export type AnyNode = Exclude<Node<any>, undefined>;
  export type AnyEnqueueResult = EnqueueResult<any>;
  export type ExtractValue<T> = T extends Queue<infer VALUE, any> | EnqueueResult<infer VALUE>
    ? VALUE
    : T extends AnyNode
      ? T["value"]
      : never;
  export type ExtractName<T> = T extends AnyQueue ? T["name"] : never;
  export type Node<VALUE> = { value: Stream.Batch<VALUE>; next?: Node<VALUE> } | undefined;
  export type DropStrategy = "newest" | "oldest";
  export type Options = {
    maxSize?: number;
    dropStrategy?: DropStrategy;
  };
  export const defaultOptions: Required<Options> = {
    maxSize: Number.MAX_SAFE_INTEGER,
    dropStrategy: "newest",
  };
  export type EnqueueResult<VALUE> = { ok: true; dropped?: never } | { ok: false; dropped: Stream.Batch<VALUE> };
  export const EMPTY = Symbol("$EMPTY#");
  export type Empty = typeof EMPTY;
}

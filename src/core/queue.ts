import { Stream } from "./stream.ts";

export class Queue<VALUE> implements Iterable<VALUE>, AsyncDisposable, Disposable {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;
  private _valueEnqueued?: Stream<VALUE, `ValueEnqueued`>;
  private _valueDequeued?: Stream<VALUE, `ValueDequeued`>;
  private _cleared?: Stream<void, `Cleared`>;
  private _disposed?: Stream<void, `Disposed`>;

  constructor() {}
  [Symbol.iterator]() {
    return {
      next: () => {
        const value = this.dequeue();
        return { value: value as VALUE, done: value === Queue.EMPTY };
      },
    };
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  enqueue(value: VALUE) {
    this._size++;
    const node = { value };
    if (!this._head) {
      this._head = this._tail = node;
    } else {
      this._tail!.next = node;
      this._tail = node;
    }
    this._valueEnqueued?.push(value);
  }
  dequeue(): VALUE | Queue.Empty {
    if (!this._head) return Queue.EMPTY;

    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    this._valueDequeued?.push(value);
    return value;
  }
  clear() {
    this._head = this._tail = undefined;
    this._cleared?.push();
  }
  async dispose() {
    this.clear();
    await Promise.all([this._valueEnqueued?.dispose(), this._valueDequeued?.dispose(), this._cleared?.dispose()]);

    this._disposed?.push();
    await this._disposed?.dispose();

    this._valueEnqueued = this._valueDequeued = this._cleared = this._disposed = undefined;
  }

  get size() {
    return this._size;
  }
  get valueEnqueued() {
    if (!this._valueEnqueued) this._valueEnqueued = new Stream(`ValueEnqueued`);
    return this._valueEnqueued;
  }
  get valueDequeued() {
    if (!this._valueDequeued) this._valueDequeued = new Stream(`ValueDequeued`);
    return this._valueDequeued;
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Stream(`Cleared`);
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`Disposed`);
    return this._disposed;
  }
}
export namespace Queue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;
  export type DropStrategy = "newest" | "oldest";
  export const EMPTY = Symbol("$QUEUE_EMPTY#");
  export type Empty = typeof EMPTY;
}

export class Queue<VALUE> implements Iterable<VALUE>, Disposable {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;

  constructor() {}
  [Symbol.iterator](): Queue.Iterator<VALUE> {
    let cursor = this._head;
    return {
      next: () => {
        if (cursor) {
          const value = cursor.value;
          cursor = cursor.next;
          return { value };
        } else {
          return { value: Queue.EMPTY, done: true };
        }
      },
    };
  }

  [Symbol.dispose](): void {
    this.clear();
  }
  enqueue(value: VALUE): this {
    this._size++;
    const node = { value };
    if (!this._head) {
      this._head = this._tail = node;
    } else {
      this._tail!.next = node;
      this._tail = node;
    }
    return this;
  }
  dequeue(): VALUE | Queue.Empty {
    if (!this._head) return Queue.EMPTY;

    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    return value;
  }
  values(): Queue.Iterator<VALUE> {
    return this[Symbol.iterator]();
  }
  clear(): void {
    this._head = this._tail = undefined;
    this._size = 0;
  }

  get size() {
    return this._size;
  }
}
export namespace Queue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;

  export type Iterator<VALUE> = {
    next: () =>
      | {
          value: VALUE;
          done?: false;
        }
      | {
          value: Empty;
          done: true;
        };
  };
  export const EMPTY = Symbol.for("empty");
  export type Empty = typeof EMPTY;
}

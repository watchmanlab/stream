import { Queue } from "./types";

export class LinkedListQueue<VALUE> implements Queue<VALUE> {
  private _head?: LinkedListQueue.Node<VALUE>;
  private _tail?: LinkedListQueue.Node<VALUE>;
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
  enqueue(value: VALUE): void {
    this._size++;
    const node = { value };
    if (!this._head) {
      this._head = this._tail = node;
    } else {
      this._tail!.next = node;
      this._tail = node;
    }
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
export namespace LinkedListQueue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;
}

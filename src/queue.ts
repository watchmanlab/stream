export class Queue<VALUE> implements Iterable<VALUE>, Disposable {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;

  constructor(private options?: Queue.Options<VALUE>) {}
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
    this.options?.enqueue?.(value);
    return this;
  }
  dequeue(): VALUE | Queue.Empty {
    if (!this._head) return Queue.EMPTY;

    this._size--;
    const value = this._head.value;
    this._head = this._head.next;

    this.options?.dequeue?.(value);

    if (!this._head) this.options?.empty?.();

    return value;
  }
  values(): Queue.Iterator<VALUE> {
    return this[Symbol.iterator]();
  }
  clear(): void {
    let array = this.options?.clear ? [...this] : [];

    this._head = this._tail = undefined;
    this._size = 0;

    this.options?.clear?.(array!);
  }

  get size() {
    return this._size;
  }
}
export namespace Queue {
  export type Node<VALUE> = { value: VALUE; next?: Node<VALUE> } | undefined;
  export type Options<VALUE> = {
    enqueue?: (value: VALUE) => void;
    dequeue?: (value: VALUE) => void;
    clear?: (values: VALUE[]) => void;
    empty?: () => void;
  };
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
  export const EMPTY = Symbol("$QUEUE_EMPTY#");
  export type Empty = typeof EMPTY;
}

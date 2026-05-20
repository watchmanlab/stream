export class Queue<VALUE> implements Iterable<VALUE>, Disposable {
  private _head?: Queue.Node<VALUE>;
  private _tail?: Queue.Node<VALUE>;
  private _size = 0;

  constructor(private options?: Queue.Options<VALUE>) {}
  [Symbol.iterator]() {
    return {
      next: () => {
        const value = this.dequeue();
        return { value: value as VALUE, done: value === Queue.EMPTY };
      },
    };
  }

  [Symbol.dispose]() {
    this.clear();
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
    this.options?.enqueue?.(value);
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
  clear(): void {
    const array = [...this];
    this._head = this._tail = undefined;
    this.options?.clear?.(array);
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
  export const EMPTY = Symbol("$QUEUE_EMPTY#");
  export type Empty = typeof EMPTY;
}

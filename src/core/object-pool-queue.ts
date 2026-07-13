import { Queue } from "./types";

// The shared pool manager instance passed into your queue instances
export class SharedNodePool<VALUE> {
  private _available: SharedNodePool.Node<VALUE>[] = [];

  constructor(initialCapacity: number) {
    for (let i = 0; i < initialCapacity; i++) {
      this._available.push({ value: undefined as any, next: undefined });
    }
  }

  public rent(value: VALUE): SharedNodePool.Node<VALUE> {
    const node = this._available.pop() ?? { value: undefined as any, next: undefined };
    node.value = value;
    return node;
  }

  public return(node: SharedNodePool.Node<VALUE>): void {
    node.value = undefined as any;
    node.next = undefined;
    this._available.push(node);
  }
}

export namespace SharedNodePool {
  export type Node<T> = { value: T; next: Node<T> | undefined };
}

// Your third clean strategy matching your contract perfectly
export class ObjectPoolQueue<VALUE> implements Queue<VALUE> {
  private _head: SharedNodePool.Node<VALUE> | undefined = undefined;
  private _tail: SharedNodePool.Node<VALUE> | undefined = undefined;
  private _size = 0;

  constructor(private _pool: SharedNodePool<VALUE>) {}

  [Symbol.iterator](): Queue.Iterator<VALUE> {
    let cursor = this._head;
    return {
      next: () => {
        if (cursor) {
          const value = cursor.value;
          cursor = cursor.next;
          return { value, done: false };
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
    // Rent a pre-existing object memory slot from the pool
    const node = this._pool.rent(value);

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
    const node = this._head;
    const value = node.value;

    this._head = this._head.next;
    if (!this._head) {
      this._tail = undefined;
    }

    // Return the object slot back to the shared pool immediately
    this._pool.return(node);

    return value;
  }

  values(): Queue.Iterator<VALUE> {
    return this[Symbol.iterator]();
  }

  clear(): void {
    let current = this._head;
    while (current) {
      const next = current.next;
      this._pool.return(current); // Evict all nodes safely back to pool
      current = next;
    }
    this._head = this._tail = undefined;
    this._size = 0;
  }

  get size() {
    return this._size;
  }
}

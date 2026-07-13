import { Queue } from "./types";

// The central coordinator hidden from the consumer
class CentralRingBuffer<VALUE> {
  public buffer: (VALUE | undefined)[];
  public capacity: number;
  public writeIndex = 0;
  // Tracks how many consumers are currently active for the entry at each index
  public refCounts: Uint32Array;

  constructor(capacity: number) {
    this.capacity = capacity;
    this.buffer = new Array(capacity);
    this.refCounts = new Uint32Array(capacity);
  }
}

export class SharedRingBufferQueue<VALUE> implements Queue<VALUE> {
  private _readIndex = 0;
  private _size = 0;

  constructor(
    private _central: CentralRingBuffer<VALUE>,
    private _consumerCount: number,
  ) {}

  enqueue(value: VALUE): void {
    const idx = this._central.writeIndex;
    this._central.buffer[idx] = value;
    // Set the initial reference count to the total number of consumers
    this._central.refCounts[idx] = this._consumerCount;

    // Move the central pointer forward (wrapping around)
    this._central.writeIndex = (idx + 1) % this._central.capacity;
  }

  dequeue(): VALUE | Queue.Empty {
    if (this._size === 0) return Queue.EMPTY;

    const idx = this._readIndex;
    const value = this._central.buffer[idx]!;

    this._size--;
    this._readIndex = (idx + 1) % this._central.capacity;

    // Decrement reference count for this slot
    this._central.refCounts[idx]--;

    // If ALL consumers have read this slot, clean it up for GC
    if (this._central.refCounts[idx] === 0) {
      this._central.buffer[idx] = undefined;
    }

    return value;
  }

  // To fulfill the rest of your interface contracts
  [Symbol.iterator](): Queue.Iterator<VALUE> {
    /* ... */ return {} as any;
  }
  values(): Queue.Iterator<VALUE> {
    return this[Symbol.iterator]();
  }
  clear(): void {
    this._size = 0;
  }
  [Symbol.dispose](): void {
    this.clear();
  }
  get size() {
    return this._size;
  }
}

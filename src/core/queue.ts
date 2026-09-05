import { Empty } from "./types";

/**
 * A FIFO queue interface used internally by {@link Consumer} to buffer values
 * when no credit is available.
 *
 * @template VALUE The type of values stored.
 */
export interface Queue<VALUE> extends Iterable<VALUE>, Disposable {
  enqueue(value: VALUE): void;
  dequeue(): VALUE | Empty;
  values(): Queue.Iterator<VALUE>;
  clear(): void;
  readonly size: number;
}
export namespace Queue {
  export interface Iterator<VALUE> {
    next: () =>
      | {
          value: VALUE;
          done?: false;
        }
      | {
          value: Empty;
          done: true;
        };
  }
}

export interface IQueue<VALUE> extends Iterable<VALUE>, Disposable {
  enqueue(value: VALUE): void;
  dequeue(): VALUE | IQueue.Empty;
  values(): IQueue.Iterator<VALUE>;
  clear(): void;
  readonly size: number;
}
export namespace IQueue {
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

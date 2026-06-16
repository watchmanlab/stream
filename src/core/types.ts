import type { Consumer } from "./consumer";

export interface Queue<VALUE> extends Iterable<VALUE>, Disposable {
  enqueue(value: VALUE): void;
  dequeue(): VALUE | Queue.Empty;
  values(): Queue.Iterator<VALUE>;
  clear(): void;
  readonly size: number;
}
export namespace Queue {
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

export interface Source<VALUE> {
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR, any>): Consumer<VALUE, ERROR, any>;
}
export namespace Source {}

export type EventShape<TYPE extends string, PROPS extends Record<string, any> = {}> = { type: TYPE } & PROPS;
export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

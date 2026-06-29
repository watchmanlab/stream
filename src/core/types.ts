import type { Consumer } from "./consumer";
import { Stream } from "./stream";

export interface Queue<VALUE> extends Iterable<VALUE>, Disposable {
  enqueue(value: VALUE): void;
  dequeue(): VALUE | Queue.Empty;
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
  export const EMPTY = Symbol.for("empty");
  export type Empty = typeof EMPTY;
}

export interface Source<VALUE> {
  listen<ERROR>(
    handler: Consumer.Handler<VALUE, ERROR>,
    options?: Consumer.Options<VALUE, ERROR>,
  ): Consumer<VALUE, ERROR>;
}
export namespace Source {
  export type AnySource = Source<any>;
}
export interface Named<NAME extends string = any> {
  readonly name: NAME;
}
export interface Evented<EVENTS extends Record<string, Stream.AnyStream> = any> {
  readonly events: EVENTS;
}

export type CloseEvents = { abort: Stream.AnyStream; complete: Stream<void, any> };
export interface Closable {
  abort(error?: any): void;
  complete(): void;
}

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

import { EMPTY, EMPTY_FUNCTION } from "./consts";
import type { Consumer } from "./consumer";
import { Source } from "./source";

import { Stream } from "./stream";

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
export interface ConsumerSet<VALUE> {
  readonly size: number;
  push(value: VALUE): void;
  add(consumer: Consumer<VALUE>): ConsumerSet.Delete;
  terminate(reason: TerminateReason): void;
}
export namespace ConsumerSet {
  export type Delete = () => boolean;
}

export type Empty = typeof EMPTY;

export type EmptyFunctions = typeof EMPTY_FUNCTION;
export type Result<VALUE, ERROR = any> =
  | { ok: true; value: VALUE; error?: never }
  | { ok: false; error: ERROR; value?: never };

export interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}

export interface Transformer<INPUT extends AnySource, VALUE> extends Source<VALUE> {
  readonly input: INPUT;
}
export type TerminateReason = "abort" | "complete";
export interface Terminable {
  readonly status: TerminateReason | (string & {});
  terminate(reason: TerminateReason): void;
}

export type NonEmptyString = `${any}${string}`;
export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export type AnyConsumable = Consumable<any>;
export type AnySource = Source<any>;
export type AnyStream = Stream<any>;
export type AnyConsumer = Consumer<any>;

export type ExtractStream<T> = T extends Stream<infer V> ? Stream<V> : never;
export type ExtractValue<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends
      | Stream<infer VALUE>
      | Consumable<infer VALUE>
      | Consumer<infer VALUE>
      | Promise<infer VALUE>
      | Array<infer VALUE>
      | Set<infer VALUE>
    ? VALUE
    : T
  : T extends
        | Stream<infer VALUE>
        | Consumable<infer VALUE>
        | Consumer<infer VALUE>
        | Promise<infer VALUE>
        | Array<infer VALUE>
        | Set<infer VALUE>
    ? ExtractValue<VALUE, DEPTH, [...COUNTER, any]>
    : T;

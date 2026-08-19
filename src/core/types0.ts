import type { EMPTY, EMPTY_FUNCTION } from "./consts";
import type { Consumer } from "./consumer0";
import type { Source } from "./source";
import type { Stream } from "./stream";

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

export interface Consumable<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}

export interface Transformer<INPUT extends AnyConsumable, VALUE> extends Source<VALUE> {
  readonly $input: INPUT;
}

export interface Terminable {
  readonly status: TerminateReason | (string & {});
  terminate(reason: TerminateReason): void;
}
export type Empty = typeof EMPTY;
export type EmptyFunctions = typeof EMPTY_FUNCTION;
export type Result<VALUE, ERROR = any> =
  | { ok: true; value: VALUE; error?: never }
  | { ok: false; error: ERROR; value?: never };
export type TerminateReason = "abort" | "complete";
export type AnyConsumable = Consumable<any>;

export type AnyTransformer = Transformer<any, any>;

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export type ExtractStream<T> = T extends Stream<infer V> ? Stream<V> : never;
export type ExtractValue<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends
      | Consumable<infer VALUE>
      | Consumer<infer VALUE>
      | Source<infer VALUE>
      | Stream<infer VALUE>
      | Transformer<any, infer VALUE>
      | Promise<infer VALUE>
      | Array<infer VALUE>
      | Set<infer VALUE>
    ? VALUE
    : T
  : T extends
        | Consumable<infer VALUE>
        | Consumer<infer VALUE>
        | Source<infer VALUE>
        | Stream<infer VALUE>
        | Transformer<any, infer VALUE>
        | Promise<infer VALUE>
        | Array<infer VALUE>
        | Set<infer VALUE>
    ? ExtractValue<VALUE, DEPTH, [...COUNTER, any]>
    : T;

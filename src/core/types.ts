import type { Consumer } from "./consumer";

import type { Stream } from "./stream";
import type { Transformer } from "./transformer";

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

export const EMPTY = Symbol.for("empty");
export type Empty = typeof EMPTY;
export interface Source<VALUE> {
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}

export interface Closable {
  readonly $terminate: Stream<"abort" | "complete", any>;
  terminate(reason: "abort" | "complete"): void;
}

export type NonEmptyString = `${any}${string}`;
export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export type AnySource = Source<any>;
export type AnyStream = Stream<any, any>;
export type AnyConsumer = Consumer<any>;
export type AnyTransformer = Transformer<any, any, any>;

////////////////////////
// | (`$${string}` & {})
export type ExtractInputStream<T extends AnyTransformer> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
export type Traversal<T extends AnyStream> = Record<T["name"], Traversable<T>>;
export type Traversable<T extends AnyStream> = T extends AnyTransformer
  ? Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>
  : T;
////////////

export type ExtractValue<T> = T extends
  | Source<infer VALUE>
  | Transformer<any, infer VALUE, any>
  | Stream<infer VALUE, any>
  | Consumer<infer VALUE>
  | Promise<infer VALUE>
  ? VALUE
  : T;

export type Transform<INPUT extends AnyStream, OUTPUT extends Transformer<INPUT, any, any> | INPUT> = (
  input: INPUT,
) => OUTPUT;

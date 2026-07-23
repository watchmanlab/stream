import type { Consumer } from "./consumer";

import type { Stream } from "./stream";
import type { Transformer } from "./transformer";

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
  listen(handler: Consumer.Handler<VALUE, any>, options?: Consumer.Options<VALUE, any>): Consumer<VALUE, any>;
}
export namespace Source {
  export type AnySource = Source<any>;
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

export type AnyStream = Stream<any, any>;
export type AnyConsumer = Consumer<any, any>;
export type AnyTransformer = Transformer<AnyStream, any, any>;
export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : T;
export type Traversal<T extends AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
export type Traversable<T extends AnyStream> = [ExtractInputStream<T>] extends [never]
  ? T
  : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;

export type ExtractValue<T> = T extends
  | Transformer<any, infer VALUE, any>
  | Stream<infer VALUE, any>
  | Source<infer VALUE>
  | Consumer<infer VALUE, any>
  | Promise<infer VALUE>
  ? VALUE
  : T;

export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

export type Transform<INPUT extends AnyStream, OUTPUT extends Transformer<INPUT, any, any> | INPUT> = (
  input: INPUT,
) => OUTPUT;

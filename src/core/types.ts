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
  listen(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE>;
}
export namespace Source {
  export type AnySource = Source<any>;
}

export interface Closable {
  readonly $terminate: Stream<"abort" | "complete">;
  terminate(reason: "abort" | "complete"): void;
}

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export type AnyStream = Stream<any>;
export type AnyConsumer = Consumer<any>;
export type AnyTransformer = Transformer<AnyStream, any>;
export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any> ? INPUT : T;
// export type Traversal<T extends AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
// export type Traversable<T extends AnyStream> = [ExtractInputStream<T>] extends [never]
//   ? T
//   : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;

export type ExtractValue<T> = T extends
  | Transformer<any, infer VALUE>
  | Stream<infer VALUE>
  | Source<infer VALUE>
  | Consumer<infer VALUE>
  | Promise<infer VALUE>
  ? VALUE
  : T;

export type Transform<INPUT extends AnyStream, OUTPUT extends Transformer<INPUT, any> | INPUT> = (
  input: INPUT,
) => OUTPUT;

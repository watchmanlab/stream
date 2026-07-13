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
export interface Named<NAME extends NonEmptyString = any> {
  readonly name: NAME;
}
export type EventStreams<EVENTS extends Record<string, unknown>, NAME extends NonEmptyString> = {
  [K in keyof EVENTS]: Stream<EVENTS[K], `${NAME}${Capitalize<K extends string ? K : "">}`>;
};
export type EventHandlers<EVENTS extends Record<string, unknown>, SELF> = {
  [K in keyof EVENTS]?: (self: SELF, value: EVENTS[K]) => void;
};
export interface Evented<EVENTS extends Record<string, any>, NAME extends NonEmptyString = NonEmptyString> {
  readonly events: EventStreams<EVENTS, NAME>;
}

export type CloseEvents = { abort: any; complete: void };
export interface Closable {
  abort(): void;
  complete(): void;
}
export type State = "active" | "drain" | "aborted" | "completed";
export interface StreamLike<VALUE, NAME extends NonEmptyString> {
  readonly stream: Stream<VALUE, NAME>;
}
export type NonEmptyString = `${any}${string}`;
export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

export type AnyTransformer = Transformer<AnyStream, any, any>;
export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
export type Traversal<T extends AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
export type Traversable<T extends AnyStream> = [ExtractInputStream<T>] extends [never]
  ? T
  : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;
export type AnyStream = Stream<any, any>;
export type ExtractValue<T> = T extends
  | Stream<infer VALUE, any>
  | Promise<infer VALUE>
  | Consumer<infer VALUE, any>
  | Source<infer VALUE>
  | Transformer<any, infer VALUE, any>
  ? VALUE
  : T;

export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

export type Transform<
  IN extends AnyStream,
  OUT_NAME extends NonEmptyString,
  OUT extends Transformer<IN, any, OUT_NAME> | IN,
> = (inputStream: IN, name?: OUT_NAME) => OUT;

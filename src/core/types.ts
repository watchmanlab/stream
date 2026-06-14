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
  listen<ERROR>(init: Source.ListenInit<VALUE, ERROR>): Source.Abort<ERROR>;
}
export namespace Source {
  export type Abort<ERROR> = (error?: ERROR) => void;
  export type Ready<ERROR> = [ERROR] extends [never] ? () => void : (error?: ERROR) => void;
  export type Error<ERROR> = (error: ERROR) => void;

  export type Handler<VALUE, ERROR> = (value: VALUE, ready: Ready<ERROR>, abort: Abort<ERROR>) => void;
  export type ListenInit<VALUE, ERROR> = {
    handler: Handler<VALUE, ERROR>;
    ready?: Ready<never>;
    abort?: Abort<ERROR>;
    error?: Error<ERROR>;
  };
}

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type FixedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : FixedArray<VALUE, SIZE, [...ARR, VALUE]>;

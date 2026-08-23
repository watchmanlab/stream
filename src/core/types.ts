import type { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import type { Consumer } from "./consumer";
import type { Source } from "./source";
import type { Stream } from "./stream";
import type { Consumable } from "./consumable";

export interface Terminable {
  readonly status: TerminateReason | (string & {});
  terminate(reason: TerminateReason): void;
}
export type Empty = typeof EMPTY;
export type EmptyFunctions = typeof EMPTY_FUNCTION;
export type EmptyThisFunctions = typeof EMPTY_THIS_FUNCTION;
export type Result<VALUE, ERROR = any> =
  | { ok: true; value: VALUE; error?: never }
  | { ok: false; error: ERROR; value?: never };
export type TerminateReason = "abort" | "complete";

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type SizedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : SizedArray<VALUE, SIZE, [...ARR, VALUE]>;
export type ZipedArray<T extends Consumable<any>[]> = [...{ [K in keyof T]: ExtractValue<T[K]> }];

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

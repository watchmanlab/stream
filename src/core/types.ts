import type { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import type { Consumer } from "./consumer";
import type { Source } from "./source";
import type { Stream } from "./stream";
import type { Consumable } from "./consumable";

export type Empty = typeof EMPTY;
export type EmptyFunctions = typeof EMPTY_FUNCTION;
export type EmptyThisFunctions = typeof EMPTY_THIS_FUNCTION;
export class Error<const VALUE> extends globalThis.Error {
  constructor(readonly value: VALUE) {
    super(typeof value === "string" ? value : "");
  }
}

export type TerminateReason = "abort" | "complete";

export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
export type SizedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : SizedArray<VALUE, SIZE, [...ARR, VALUE]>;
export type ZipedArray<T extends Consumable<any>[]> = [...{ [K in keyof T]: ValueOfConsumable<T[K]> }];

export type ExtractStream<T> = T extends Stream<infer V> ? Stream<V> : never;

export type ValueOfConsumable<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends Consumable<infer VALUE>
    ? VALUE
    : T
  : T extends Consumable<infer VALUE>
    ? ValueOfConsumable<VALUE, DEPTH, [...COUNTER, any]>
    : T;
export type ValueOfPromise<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends Promise<infer VALUE>
    ? VALUE
    : T
  : T extends Promise<infer VALUE>
    ? ValueOfPromise<VALUE, DEPTH, [...COUNTER, any]>
    : T;
export type ValueOfArray<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends Array<infer VALUE>
    ? VALUE
    : T
  : T extends Array<infer VALUE>
    ? ValueOfArray<VALUE, DEPTH, [...COUNTER, any]>
    : T;

export type ValueOfError<T, DEPTH extends number = 0, COUNTER extends any[] = []> = COUNTER["length"] extends DEPTH
  ? T extends Error<infer VALUE>
    ? VALUE
    : T
  : T extends Error<infer VALUE>
    ? ValueOfArray<VALUE, DEPTH, [...COUNTER, any]>
    : T;

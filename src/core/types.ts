import type { EMPTY, EMPTY_FUNCTION, EMPTY_THIS_FUNCTION } from "./consts";
import { Stream } from "./stream";
import type { Consumable } from "./consumable";
import { Source } from "./source";

export function isConsumable<T>(object: unknown): object is Consumable<T> {
  if (object instanceof Source || object instanceof Stream) return true;

  return typeof object === "object" && object !== null && "consume" in object && typeof object.consume === "function";
}

/** The type of the {@link EMPTY} sentinel symbol. */
export type Empty = typeof EMPTY;
export type EmptyFunctions = typeof EMPTY_FUNCTION;
export type EmptyThisFunctions = typeof EMPTY_THIS_FUNCTION;

/**
 * A typed error wrapper that carries a value instead of a message string.
 * Used as the error channel in pipelines — errors are just values.
 *
 * @template VALUE The type of the wrapped error value.
 */
export class Error<const VALUE> extends globalThis.Error {
  constructor(readonly value: VALUE) {
    super(typeof value === "string" ? value : "");
  }
}

/** Reason a stream or consumer was terminated. */
export type TerminateReason = "abort" | "complete";

/** Flattens an object type for better IDE display. */
export type Prettify<T> = T extends { [K in keyof T]: T[K] } ? { [K in keyof T]: T[K] } : never;
/** Creates a tuple type of `VALUE` repeated `SIZE` times. */
export type SizedArray<VALUE, SIZE extends number = 2, ARR extends Array<VALUE> = []> = ARR["length"] extends SIZE
  ? ARR
  : SizedArray<VALUE, SIZE, [...ARR, VALUE]>;
/** Extracts the value types from a tuple of `Consumable`s. */
export type ZipedArray<T extends Consumable<any>[]> = [...{ [K in keyof T]: ValueOfConsumable<T[K]> }];

export type ExtractStream<T> = T extends Stream<infer V> ? Stream<V> : never;

/** Recursively extracts the value type from nested `Consumable`s up to `DEPTH` levels. */
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

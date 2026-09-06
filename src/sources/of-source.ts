import { ValueOfArray } from "../core/types";
import { fromIterable, type IterableSource } from "./iterable-source";

/**
 * `Replayable`
 *
 * Emits a fixed list of values then completes. Shorthand for `fromIterable(values)`.
 *
 * @example
 * of(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
 */
export function of<VALUES extends [value: any, ...values: any[]]>(
  ...values: VALUES
): IterableSource<ValueOfArray<VALUES>> {
  return fromIterable(values);
}

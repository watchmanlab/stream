import { ValueOfArray } from "../core/types";
import { fromIterable, type IterableSource } from "./iterable-source";

export function of<VALUES extends [value: any, ...values: any[]]>(
  ...values: VALUES
): IterableSource<ValueOfArray<VALUES>> {
  return fromIterable(values);
}
